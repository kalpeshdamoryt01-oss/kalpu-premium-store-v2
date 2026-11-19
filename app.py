import os
import io
import hmac
import html
import json
import logging
import sqlite3
import secrets
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from urllib.parse import urlencode

import qrcode
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from starlette.middleware.sessions import SessionMiddleware
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)

# ---------------- Configuration ----------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
ADMIN_USERNAME = os.environ["ADMIN_USERNAME"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]
SESSION_SECRET = os.environ["SESSION_SECRET"]
UPI_ID = os.environ["UPI_ID"]
UPI_PAYEE_NAME = os.getenv("UPI_PAYEE_NAME", "Kalpu Store")
STORE_NAME = os.getenv("STORE_NAME", "KALPU PREMIUM STORE")
DB_PATH = os.getenv("DB_PATH", "/var/data/store.db")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "")
CURRENCY = "INR"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("kalpu-store")

db_lock = asyncio.Lock()
bot_app: Application | None = None


# ---------------- Database ----------------
class ClosingConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc, tb):
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


def connect_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=20, factory=ClosingConnection)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def init_db():
    with connect_db() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            price INTEGER NOT NULL CHECK(price > 0),
            old_price INTEGER,
            delivery_url TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL DEFAULT '',
            product_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            utr TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'awaiting_utr',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            delivered_at TEXT,
            FOREIGN KEY(product_id) REFERENCES products(id)
        );

        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
        CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
        """)

        count = con.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        if count == 0:
            with open("products.json", "r", encoding="utf-8") as f:
                seed = json.load(f)
            for p in seed:
                con.execute("""
                    INSERT INTO products(name, description, price, old_price,
                                         delivery_url, active, created_at)
                    VALUES (?, ?, ?, ?, ?, 1, ?)
                """, (
                    p["name"], p.get("description", ""), int(p["price"]),
                    p.get("old_price"), p["delivery_url"], utcnow()
                ))


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_products(active_only=True):
    with connect_db() as con:
        sql = "SELECT * FROM products"
        if active_only:
            sql += " WHERE active=1"
        return con.execute(sql + " ORDER BY id DESC").fetchall()


def get_product(product_id):
    with connect_db() as con:
        return con.execute(
            "SELECT * FROM products WHERE id=?", (product_id,)
        ).fetchone()


def create_order(user_id, username, product_id):
    product = get_product(product_id)
    if not product or not product["active"]:
        raise ValueError("Product unavailable")
    now = utcnow()
    with connect_db() as con:
        cur = con.execute("""
            INSERT INTO orders(user_id, username, product_id, amount,
                               status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'awaiting_utr', ?, ?)
        """, (user_id, username or "", product_id, product["price"], now, now))
        return cur.lastrowid


def get_order(order_id):
    with connect_db() as con:
        return con.execute("""
            SELECT o.*, p.name AS product_name, p.delivery_url
            FROM orders o JOIN products p ON p.id=o.product_id
            WHERE o.id=?
        """, (order_id,)).fetchone()


def set_utr(order_id, user_id, utr):
    with connect_db() as con:
        cur = con.execute("""
            UPDATE orders SET utr=?, status='pending_review', updated_at=?
            WHERE id=? AND user_id=? AND status='awaiting_utr'
        """, (utr, utcnow(), order_id, user_id))
        return cur.rowcount == 1


def update_product(product_id, name, description, price, old_price, url, active):
    with connect_db() as con:
        con.execute("""
            UPDATE products SET name=?, description=?, price=?, old_price=?,
                                delivery_url=?, active=?
            WHERE id=?
        """, (name, description, price, old_price, url, active, product_id))


def add_product(name, description, price, old_price, url):
    with connect_db() as con:
        con.execute("""
            INSERT INTO products(name, description, price, old_price,
                                 delivery_url, active, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
        """, (name, description, price, old_price, url, utcnow()))


def set_order_status(order_id, status):
    with connect_db() as con:
        cur = con.execute("""
            UPDATE orders SET status=?, updated_at=?
            WHERE id=? AND status='pending_review'
        """, (status, utcnow(), order_id))
        return cur.rowcount == 1


def mark_delivered(order_id):
    with connect_db() as con:
        con.execute("""
            UPDATE orders SET status='delivered', delivered_at=?, updated_at=?
            WHERE id=? AND status='approved'
        """, (utcnow(), utcnow(), order_id))


def list_orders(limit=100):
    with connect_db() as con:
        return con.execute("""
            SELECT o.*, p.name AS product_name
            FROM orders o JOIN products p ON p.id=o.product_id
            ORDER BY o.id DESC LIMIT ?
        """, (limit,)).fetchall()


def order_stats():
    with connect_db() as con:
        total = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        pending = con.execute(
            "SELECT COUNT(*) FROM orders WHERE status='pending_review'"
        ).fetchone()[0]
        delivered = con.execute(
            "SELECT COUNT(*) FROM orders WHERE status='delivered'"
        ).fetchone()[0]
        revenue = con.execute(
            "SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='delivered'"
        ).fetchone()[0]
    return total, pending, delivered, revenue


# ---------------- Shared helpers ----------------
def money(amount):
    return f"₹{int(amount)}"


def safe(text):
    return html.escape(str(text or ""))


def upi_uri(order_id, amount):
    params = {
        "pa": UPI_ID,
        "pn": UPI_PAYEE_NAME,
        "am": f"{amount:.2f}",
        "cu": CURRENCY,
        "tn": f"{STORE_NAME} Order {order_id}"
    }
    return "upi://pay?" + urlencode(params)


def make_qr(order_id, amount):
    img = qrcode.make(upi_uri(order_id, amount))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf


def is_admin(user_id):
    return int(user_id) == ADMIN_ID


async def notify_admin(order, context):
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve + deliver", callback_data=f"approve:{order['id']}"),
        InlineKeyboardButton("❌ Reject", callback_data=f"reject:{order['id']}")
    ]])
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"🧾 PAYMENT REVIEW — Order #{order['id']}\n\n"
            f"Customer ID: {order['user_id']}\n"
            f"Username: @{order['username'] or 'not set'}\n"
            f"Product: {order['product_name']}\n"
            f"Amount: {money(order['amount'])}\n"
            f"UTR: {order['utr']}\n\n"
            "Check your UPI/bank statement before approving."
        ),
        reply_markup=keyboard
    )


async def deliver_order(order_id, context):
    order = get_order(order_id)
    if not order or order["status"] != "approved":
        return False
    # Mark first to prevent duplicate deliveries if callbacks are repeated.
    with connect_db() as con:
        cur = con.execute("""
            UPDATE orders SET status='delivered', delivered_at=?, updated_at=?
            WHERE id=? AND status='approved'
        """, (utcnow(), utcnow(), order_id))
        if cur.rowcount != 1:
            return False

    try:
        await context.bot.send_message(
            chat_id=order["user_id"],
            text=(
                f"🎉 Payment verified — order #{order_id}\n\n"
                f"📦 {order['product_name']}\n"
                f"📥 Your product:\n{order['delivery_url']}\n\n"
                "Thank you for your purchase!"
            ),
            disable_web_page_preview=True
        )
        return True
    except Exception:
        # Keep the order delivered to avoid duplicate links; admin can resend manually.
        log.exception("Delivery message failed for order %s", order_id)
        return False


# ---------------- Telegram bot ----------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = []
    for p in get_products():
        label = f"{p['name']} — {money(p['price'])}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"buy:{p['id']}")])
    if not keyboard:
        await update.message.reply_text("Store is updating. Please try again later.")
        return
    await update.message.reply_text(
        f"🔥 {STORE_NAME} 🔥\n\nChoose a product below.\n"
        "Pay by UPI and submit your transaction reference.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def buy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    product_id = int(query.data.split(":")[1])
    product = get_product(product_id)
    if not product or not product["active"]:
        await query.message.reply_text("This product is currently unavailable.")
        return

    order_id = create_order(
        query.from_user.id, query.from_user.username or "", product_id
    )
    qr = make_qr(order_id, product["price"])
    await query.message.reply_photo(
        photo=qr,
        caption=(
            f"🛒 Order #{order_id}\n"
            f"Product: {product['name']}\n"
            f"Amount: {money(product['price'])}\n\n"
            f"UPI ID: {UPI_ID}\n"
            "Scan the QR in your UPI app. After paying, send the 12-digit UTR "
            "or transaction reference in this chat.\n\n"
            "⚠️ Do not submit a reference until payment is completed. "
            "Delivery follows admin verification."
        )
    )
    context.user_data["active_order_id"] = order_id


async def utr_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    order_id = context.user_data.get("active_order_id")
    if not order_id:
        return
    utr = (update.message.text or "").strip()
    if not (6 <= len(utr) <= 40) or not utr.replace("-", "").isalnum():
        await update.message.reply_text(
            "Send a valid transaction reference (6–40 letters/numbers)."
        )
        return

    ok = set_utr(order_id, update.effective_user.id, utr)
    context.user_data.pop("active_order_id", None)
    if not ok:
        await update.message.reply_text(
            "This order is no longer awaiting payment details. Use /start to shop again."
        )
        return

    order = get_order(order_id)
    await update.message.reply_text(
        f"✅ Reference received for order #{order_id}. "
        "Your payment is waiting for verification."
    )
    await notify_admin(order, context)


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Not authorized.")
        return
    total, pending, delivered, revenue = order_stats()
    await update.message.reply_text(
        "🛠 ADMIN PANEL\n\n"
        f"Orders: {total}\nPending review: {pending}\n"
        f"Delivered: {delivered}\nRevenue delivered: {money(revenue)}\n\n"
        "Use the web dashboard for product and order management."
    )


async def admin_order_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Unauthorized", show_alert=True)
        return
    await query.answer()
    action, raw_id = query.data.split(":")
    order_id = int(raw_id)
    order = get_order(order_id)
    if not order or order["status"] != "pending_review":
        await query.edit_message_text("This order has already been processed.")
        return

    if action == "approve":
        if not set_order_status(order_id, "approved"):
            await query.edit_message_text("Order already processed.")
            return
        delivered = await deliver_order(order_id, context)
        await query.edit_message_text(
            f"✅ Order #{order_id} approved. "
            + ("Delivery sent." if delivered else "Delivery attempt failed; check logs.")
        )
    elif action == "reject":
        if not set_order_status(order_id, "rejected"):
            await query.edit_message_text("Order already processed.")
            return
        await context.bot.send_message(
            order["user_id"],
            f"❌ Payment for order #{order_id} could not be verified. "
            "Contact support if you believe this is an error."
        )
        await query.edit_message_text(f"❌ Order #{order_id} rejected.")


# ---------------- Web admin ----------------
def page(title, body, request=None):
    nav = ""
    if request and request.session.get("admin"):
        nav = """
        <nav><a href="/">Dashboard</a> · <a href="/products">Products</a>
        · <a href="/logout">Logout</a></nav>
        """
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{safe(title)}</title>
<style>
:root{{--bg:#101522;--panel:#192235;--text:#edf2fa;--muted:#aab6c8;--accent:#19b65b;--red:#d95c5c;}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--text);font:15px system-ui,Arial}}
main{{max-width:1100px;margin:28px auto;padding:0 16px}} .card{{background:var(--panel);padding:18px;border-radius:14px;margin:14px 0;overflow:auto}}
h1,h2{{margin-top:0}} a{{color:#75c5ff}} nav{{margin:14px 0;color:var(--muted)}}
input,textarea{{width:100%;padding:11px;margin:6px 0 14px;border:1px solid #40506a;border-radius:8px;background:#0e1727;color:white}}
button{{border:0;border-radius:8px;padding:10px 14px;background:var(--accent);color:#07140b;font-weight:700;cursor:pointer}}
button.danger{{background:var(--red);color:white}} table{{width:100%;border-collapse:collapse;min-width:700px}}
td,th{{padding:10px;border-bottom:1px solid #344157;text-align:left;vertical-align:top}}
small,.muted{{color:var(--muted)}} .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}}
.stat{{background:#0f1a2b;padding:14px;border-radius:10px}} .stat b{{display:block;font-size:24px}}
.inline{{display:inline-block;margin-right:6px}} code{{overflow-wrap:anywhere}}
</style></head><body><main><h1>{safe(STORE_NAME)}</h1>{nav}{body}</main></body></html>"""


def require_login(request):
    if not request.session.get("admin"):
        raise HTTPException(status_code=303, headers={"Location": "/login"})


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot_app
    init_db()
    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start_cmd))
    bot_app.add_handler(CommandHandler("admin", admin_cmd))
    bot_app.add_handler(CallbackQueryHandler(buy_callback, pattern=r"^buy:\d+$"))
    bot_app.add_handler(CallbackQueryHandler(
        admin_order_callback, pattern=r"^(approve|reject):\d+$"
    ))
    bot_app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, utr_message
    ))
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)
    log.info("Telegram bot polling started")
    yield
    if bot_app:
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()


app = FastAPI(title="Kalpu Store Admin", lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="kalpu_admin",
    max_age=60 * 60 * 8,
    same_site="lax",
    https_only=True
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    body = """
    <div class="card"><h2>Admin Login</h2>
    <form method="post" action="/login">
    <label>Username</label><input name="username" required autocomplete="username">
    <label>Password</label><input name="password" type="password" required autocomplete="current-password">
    <button type="submit">Login</button></form></div>
    """
    return page("Login", body)


@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user_ok = hmac.compare_digest(username, ADMIN_USERNAME)
    pass_ok = hmac.compare_digest(password, ADMIN_PASSWORD)
    if not (user_ok and pass_ok):
        return HTMLResponse(page("Login failed", '<div class="card">Invalid credentials. <a href="/login">Try again</a></div>'), status_code=401)
    request.session.clear()
    request.session["admin"] = True
    request.session["csrf"] = secrets.token_urlsafe(24)
    return RedirectResponse("/", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


def csrf_field(request):
    return f'<input type="hidden" name="csrf" value="{safe(request.session.get("csrf",""))}">'


def check_csrf(request, token):
    expected = request.session.get("csrf", "")
    if not expected or not hmac.compare_digest(expected, token or ""):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    require_login(request)
    total, pending, delivered, revenue = order_stats()
    rows = []
    for o in list_orders():
        actions = ""
        if o["status"] == "pending_review":
            actions = f"""
            <form class="inline" method="post" action="/orders/{o['id']}/approve">{csrf_field(request)}
            <button>Approve + deliver</button></form>
            <form class="inline" method="post" action="/orders/{o['id']}/reject">{csrf_field(request)}
            <button class="danger">Reject</button></form>"""
        rows.append(
            f"<tr><td>#{o['id']}</td><td>{safe(o['product_name'])}</td>"
            f"<td>{safe(o['username'])} ({o['user_id']})</td>"
            f"<td>{money(o['amount'])}</td><td>{safe(o['utr'])}</td>"
            f"<td>{safe(o['status'])}</td><td>{actions}</td></tr>"
        )
    body = f"""
    <div class="grid">
      <div class="stat">Total orders<b>{total}</b></div>
      <div class="stat">Awaiting review<b>{pending}</b></div>
      <div class="stat">Delivered<b>{delivered}</b></div>
      <div class="stat">Delivered revenue<b>{money(revenue)}</b></div>
    </div>
    <div class="card"><h2>Orders</h2>
    <table><thead><tr><th>Order</th><th>Product</th><th>Customer</th><th>Amount</th>
    <th>UTR</th><th>Status</th><th>Action</th></tr></thead>
    <tbody>{''.join(rows) or '<tr><td colspan="7">No orders yet.</td></tr>'}</tbody></table></div>
    <p class="muted">Approve only after confirming the payment in your bank/UPI app.</p>
    """
    return page("Dashboard", body, request)


@app.post("/orders/{order_id}/{action}")
async def web_order_action(
    request: Request, order_id: int, action: str, csrf: str = Form(...)
):
    require_login(request)
    check_csrf(request, csrf)
    if action not in ("approve", "reject"):
        raise HTTPException(status_code=404)
    order = get_order(order_id)
    if not order or order["status"] != "pending_review":
        return RedirectResponse("/", status_code=303)

    new_status = "approved" if action == "approve" else "rejected"
    if not set_order_status(order_id, new_status):
        return RedirectResponse("/", status_code=303)

    if action == "approve":
        if bot_app:
            await deliver_order(order_id, bot_app)
    else:
        if bot_app:
            await bot_app.bot.send_message(
                order["user_id"],
                f"❌ Payment for order #{order_id} could not be verified. Contact support."
            )
    return RedirectResponse("/", status_code=303)


@app.get("/products", response_class=HTMLResponse)
async def products_page(request: Request):
    require_login(request)
    rows = []
    for p in get_products(active_only=False):
        rows.append(
            f"<tr><td>{p['id']}</td><td>{safe(p['name'])}</td>"
            f"<td>{money(p['price'])}</td><td>{'Active' if p['active'] else 'Hidden'}</td>"
            f"<td><code>{safe(p['delivery_url'])}</code></td></tr>"
        )
    body = f"""
    <div class="card"><h2>Add product</h2>
    <form method="post" action="/products">
    {csrf_field(request)}
    <label>Name</label><input name="name" maxlength="100" required>
    <label>Description</label><textarea name="description" maxlength="500"></textarea>
    <label>Price (whole rupees)</label><input name="price" type="number" min="1" max="100000" required>
    <label>Old price (optional)</label><input name="old_price" type="number" min="1" max="100000">
    <label>Delivery URL (HTTPS only)</label><input name="delivery_url" type="url" required>
    <button>Add product</button></form></div>
    <div class="card"><h2>Catalogue</h2><table><tr><th>ID</th><th>Name</th><th>Price</th><th>Status</th><th>Delivery URL</th></tr>
    {''.join(rows)}</table></div>
    """
    return page("Products", body, request)


@app.post("/products")
async def add_product_web(
    request: Request,
    name: str = Form(...),
    description: str = Form(""),
    price: int = Form(...),
    old_price: str = Form(""),
    delivery_url: str = Form(...),
    csrf: str = Form(...)
):
    require_login(request)
    check_csrf(request, csrf)
    name = name.strip()
    url = delivery_url.strip()
    if not name or len(name) > 100 or len(description) > 500:
        raise HTTPException(status_code=400, detail="Invalid product details")
    if price < 1 or price > 100000:
        raise HTTPException(status_code=400, detail="Invalid price")
    if not url.startswith("https://"):
        raise HTTPException(status_code=400, detail="Delivery URL must use HTTPS")
    old = int(old_price) if old_price.strip() else None
    if old is not None and (old < 1 or old > 100000):
        raise HTTPException(status_code=400, detail="Invalid old price")
    add_product(name, description, price, old, url)
    return RedirectResponse("/products", status_code=303)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
