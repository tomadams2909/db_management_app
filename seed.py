"""
Seed the LOCAL database with example data on first run.
Idempotent — skips if the customers table already has rows.
"""
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

FIRST_NAMES = ["Alice", "Bob", "Carol", "Dan", "Eve", "Frank", "Grace", "Hank",
               "Iris", "Jack", "Karen", "Leo", "Mia", "Ned", "Olivia", "Pete",
               "Quinn", "Rose", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
               "Yara", "Zoe", "Aaron", "Beth", "Chris", "Diana"]
LAST_NAMES  = ["Smith", "Jones", "Williams", "Brown", "Taylor", "Davies", "Evans",
               "Wilson", "Thomas", "Roberts", "Johnson", "White", "Martin", "Lee",
               "Thompson", "Clark", "Lewis", "Robinson", "Walker", "Hall"]
CITIES      = ["London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Bristol",
               "Liverpool", "Edinburgh", "Cardiff", "Sheffield", "Newcastle", "Oxford"]
CATEGORIES  = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys"]
STATUSES    = ["pending", "processing", "shipped", "delivered", "cancelled"]

PRODUCTS = [
    ("Wireless Headphones",  "Electronics",   79.99,  143),
    ("Running Shoes",        "Sports",         64.99,  210),
    ("Desk Lamp",            "Home & Garden",  34.99,   87),
    ("Python Cookbook",      "Books",          29.99,  305),
    ("Yoga Mat",             "Sports",         24.99,  178),
    ("Mechanical Keyboard",  "Electronics",   109.99,   62),
    ("Coffee Maker",         "Home & Garden",  49.99,   94),
    ("Backpack",             "Clothing",       44.99,  156),
    ("LEGO City Set",        "Toys",           39.99,  220),
    ("USB-C Hub",            "Electronics",    29.99,  311),
    ("Notebook Set",         "Books",           9.99,  500),
    ("Water Bottle",         "Sports",         14.99,  430),
    ("Table Lamp",           "Home & Garden",  44.99,   73),
    ("Winter Jacket",        "Clothing",       89.99,   48),
    ("Board Game",           "Toys",           34.99,  195),
]


def _random_date(days_back: int) -> str:
    d = datetime.utcnow() - timedelta(days=random.randint(0, days_back))
    return d.strftime("%Y-%m-%d")


def run(db_url: str) -> None:
    engine = create_engine(db_url)
    rng = random.Random(42)

    with engine.begin() as conn:
        # Already seeded — skip
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customers (
                id         SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name  TEXT NOT NULL,
                email      TEXT NOT NULL,
                phone      TEXT,
                city       TEXT,
                signup_date DATE
            )
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        if count > 0:
            return

        # ── Customers ──────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS products (
                id       SERIAL PRIMARY KEY,
                name     TEXT NOT NULL,
                category TEXT,
                price    NUMERIC(8,2),
                stock    INTEGER DEFAULT 0
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS orders (
                id          SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id),
                product_id  INTEGER REFERENCES products(id),
                quantity    INTEGER NOT NULL DEFAULT 1,
                status      TEXT,
                order_date  DATE,
                total       NUMERIC(10,2)
            )
        """))

        customers = []
        for i in range(50):
            fn = rng.choice(FIRST_NAMES)
            ln = rng.choice(LAST_NAMES)
            email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
            phone = f"07{rng.randint(100000000, 999999999)}"
            city  = rng.choice(CITIES)
            date  = _random_date(730)
            customers.append({"fn": fn, "ln": ln, "email": email,
                               "phone": phone, "city": city, "date": date})

        conn.execute(text("""
            INSERT INTO customers (first_name, last_name, email, phone, city, signup_date)
            VALUES (:fn, :ln, :email, :phone, :city, :date)
        """), customers)

        # ── Products ───────────────────────────────────────────────────────
        for name, cat, price, stock in PRODUCTS:
            conn.execute(text("""
                INSERT INTO products (name, category, price, stock)
                VALUES (:name, :cat, :price, :stock)
            """), {"name": name, "cat": cat, "price": price, "stock": stock})

        # ── Orders ─────────────────────────────────────────────────────────
        orders = []
        for _ in range(150):
            cust_id = rng.randint(1, 50)
            prod_id = rng.randint(1, len(PRODUCTS))
            qty     = rng.randint(1, 5)
            price   = PRODUCTS[prod_id - 1][2]
            orders.append({
                "cid":    cust_id,
                "pid":    prod_id,
                "qty":    qty,
                "status": rng.choice(STATUSES),
                "date":   _random_date(365),
                "total":  round(price * qty, 2),
            })

        conn.execute(text("""
            INSERT INTO orders (customer_id, product_id, quantity, status, order_date, total)
            VALUES (:cid, :pid, :qty, :status, :date, :total)
        """), orders)
