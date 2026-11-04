"""Sinh du lieu nguon (landing/) va answer key cho bai Medallion.

Moi tinh ngau nhien dung seed trong config.yaml, chay lai cho ra tep giong het.
Chay: python generate_data.py [--output-dir DIR]
"""

import random
from dataclasses import dataclass, field

from common.config import load_config

# ---------------------------------------------------------------------------
# Cot cua tung thuc the (dung ten, dung thu tu SPEC 6.1)
# ---------------------------------------------------------------------------
ORDER_COLUMNS = ["order_id", "customer_id", "order_date", "total_amount",
                 "status", "channel", "updated_at"]
ITEM_COLUMNS = ["order_id", "product_id", "quantity", "unit_price"]
CUSTOMER_COLUMNS = ["customer_id", "name", "email", "phone", "city"]
PRODUCT_COLUMNS = ["product_id", "name", "category", "list_price"]

# ---------------------------------------------------------------------------
# Tu dien de sinh ten
# ---------------------------------------------------------------------------
LAST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Huynh", "Phan", "Vu",
              "Vo", "Dang", "Bui", "Do", "Ho", "Ngo", "Duong", "Ly"]
MIDDLE_NAMES = ["Van", "Thi", "Minh", "Ngoc", "Thanh", "Duc", "Hoai", "Quoc"]
FIRST_NAMES = ["An", "Binh", "Chi", "Dung", "Giang", "Ha", "Hai", "Hieu", "Hoa",
               "Hung", "Khanh", "Lan", "Linh", "Long", "Mai", "Nam", "Phuong",
               "Quan", "Son", "Tam", "Thao", "Trang", "Tuan", "Vy"]
CITIES = ["Ha Noi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Hue",
          "Nha Trang", "Vung Tau"]
PRODUCT_WORDS = {
    "Electronics": ["Tai nghe", "Sac du phong", "Loa bluetooth", "Chuot khong day",
                    "Ban phim", "Dong ho thong minh"],
    "Fashion": ["Ao thun", "Quan jean", "Vay lien", "Giay sneaker", "Tui xach", "Mu luoi trai"],
    "Home": ["Noi chien", "Am sieu toc", "Den ban", "Goi om", "Bo chen", "Thung rac"],
    "Beauty": ["Son moi", "Kem chong nang", "Sua rua mat", "Nuoc hoa", "Mat na", "Dau goi"],
    "Sports": ["Tham yoga", "Ta tay", "Bong da", "Vot cau long", "Binh nuoc", "Day nhay"],
    "Books": ["Tieu thuyet", "Sach nau an", "Truyen tranh", "Sach ky nang",
              "Tu dien", "Sach thieu nhi"],
}
# khoang gia (VND, boi so 5.000) theo danh muc
PRICE_RANGES = {
    "Electronics": (200_000, 2_000_000), "Fashion": (100_000, 800_000),
    "Home": (80_000, 1_500_000), "Beauty": (60_000, 600_000),
    "Sports": (50_000, 900_000), "Books": (50_000, 300_000),
}
PRICE_STEP = 5_000

# san pham dung cho ban ghi minh hoa (gia co dinh de tong = 150000)
SHOWCASE_PRODUCT_PRICES = {"P-001": ("Books", 50_000), "P-002": ("Beauty", 75_000)}


# ---------------------------------------------------------------------------
# Cau truc du lieu
# ---------------------------------------------------------------------------
@dataclass
class LandingRow:
    """Mot dong trong tep landing.

    values: gia tri da kieu hoa (sau khi cai loi lam doi gia tri, vd quantity <= 0).
    errors: danh sach ma loi da cai vao dong nay.
    """
    entity: str
    values: dict
    errors: list = field(default_factory=list)
    channel: str = None          # chi dung cho orders: tep kenh nao
    raw: dict = field(default_factory=dict)    # chuoi ghi de khi ghi tep (loi dinh dang)
    clean: dict = None           # gia tri sach truoc khi cai loi

    def __post_init__(self):
        """Luu ban sach ngay khi tao dong."""
        if self.clean is None:
            self.clean = dict(self.values)

    def add_error(self, code, **raw_overrides):
        """Ghi nhan mot ma loi va (neu co) chuoi ghi de cho cot."""
        if code not in self.errors:
            self.errors.append(code)
        self.raw.update(raw_overrides)

    def to_raw(self, columns):
        """Doi dong thanh dict chuoi theo dung thu tu cot (dung khi ghi tep)."""
        out = {}
        for col in columns:
            if col in self.raw:
                out[col] = self.raw[col]
            else:
                v = self.values.get(col)
                out[col] = "" if v is None else str(v)
        return out


# don minh hoa dung trong reading (SPEC 6.4)
SHOWCASE_IDS = ("ORD-1001", "ORD-1002")


# ---------------------------------------------------------------------------
# Tien ich ngau nhien
# ---------------------------------------------------------------------------
def make_rng(seed, label):
    """Tao Random rieng cho tung muc dich, on dinh qua cac lan chay."""
    return random.Random(f"{seed}:{label}")


def count_for(rate, n):
    """So phan tu can chon theo ty le (lam tron, it nhat 1 neu rate > 0)."""
    if n <= 0 or rate <= 0:
        return 0
    return max(1, int(rate * n + 0.5))


def random_price(rng, category):
    """Gia ngau nhien trong khoang cua danh muc, boi so PRICE_STEP."""
    low, high = PRICE_RANGES[category]
    return rng.randrange(low, high + 1, PRICE_STEP)


# ---------------------------------------------------------------------------
# Products va customers sach
# ---------------------------------------------------------------------------
def make_products(cfg):
    """Sinh danh muc san pham sach P-001..P-NNN."""
    rng = make_rng(cfg["seed"], "products")
    categories = cfg["volume"]["categories"]
    products = []
    for i in range(1, cfg["volume"]["n_products"] + 1):
        product_id = f"P-{i:03d}"
        if product_id in SHOWCASE_PRODUCT_PRICES:
            category, price = SHOWCASE_PRODUCT_PRICES[product_id]
        else:
            category = categories[(i - 1) % len(categories)]
            price = random_price(rng, category)
        word = rng.choice(PRODUCT_WORDS[category])
        products.append({"product_id": product_id, "name": f"{word} {i:03d}",
                         "category": category, "list_price": price})
    return products


def make_customer(rng, index):
    """Sinh mot khach hang sach voi ma C-<index>."""
    last, middle, first = (rng.choice(LAST_NAMES), rng.choice(MIDDLE_NAMES),
                           rng.choice(FIRST_NAMES))
    customer_id = f"C-{index:03d}"
    email = f"{first.lower()}.{last.lower()}{index:03d}@example.com"
    phone = "09" + "".join(str(rng.randrange(10)) for _ in range(8))
    return {"customer_id": customer_id, "name": f"{last} {middle} {first}",
            "email": email, "phone": phone, "city": rng.choice(CITIES)}


def make_master_data(cfg):
    """Sinh products va customers cho tung ngay.

    Ngay 1: full snapshot. Cac ngay sau: khach moi + khach doi city,
    san pham doi gia. Tra ve dict date -> {"products": [...], "customers": [...]}
    (moi phan tu la dict gia tri sach).
    """
    vol = cfg["volume"]
    dates = cfg["dates"]
    rng = make_rng(cfg["seed"], "master_updates")
    products = make_products(cfg)
    product_state = {p["product_id"]: dict(p) for p in products}
    customers = [make_customer(rng, i) for i in range(1, vol["n_customers"] + 1)]
    customer_state = {c["customer_id"]: dict(c) for c in customers}

    result = {dates[0]: {"products": [dict(p) for p in products],
                         "customers": [dict(c) for c in customers]}}
    next_customer = vol["n_customers"] + 1
    for date in dates[1:]:
        # san pham doi gia (+/- toi da 10%, giu boi so PRICE_STEP)
        changed_products = []
        candidates = [pid for pid in sorted(product_state) if pid not in SHOWCASE_PRODUCT_PRICES]
        for pid in sorted(rng.sample(candidates, vol["updated_products_per_day"])):
            p = dict(product_state[pid])
            factor = 1 + rng.choice([-1, 1]) * rng.randint(1, 10) / 100
            p["list_price"] = max(PRICE_STEP, round(p["list_price"] * factor / PRICE_STEP) * PRICE_STEP)
            product_state[pid] = p
            changed_products.append(dict(p))

        # khach cu doi city
        changed_customers = []
        for cid in sorted(rng.sample(sorted(customer_state), vol["updated_customers_per_day"])):
            c = dict(customer_state[cid])
            c["city"] = rng.choice([x for x in CITIES if x != c["city"]])
            customer_state[cid] = c
            changed_customers.append(dict(c))

        # khach moi
        for _ in range(vol["new_customers_per_day"]):
            c = make_customer(rng, next_customer)
            next_customer += 1
            customer_state[c["customer_id"]] = c
            changed_customers.append(dict(c))

        result[date] = {"products": changed_products, "customers": changed_customers}
    return result


# ---------------------------------------------------------------------------
# Orders va order_items sach
# ---------------------------------------------------------------------------
def weighted_choice(rng, weights):
    """Chon mot khoa trong dict weights theo ty trong."""
    keys = list(weights)
    return rng.choices(keys, weights=[weights[k] for k in keys], k=1)[0]


def random_time(rng, date, start_hour=8, end_hour=21):
    """Thoi diem ngau nhien trong ngay, dang 'YYYY-MM-DD HH:MM:SS'."""
    seconds = rng.randrange(start_hour * 3600, end_hour * 3600)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{date} {h:02d}:{m:02d}:{s:02d}"


def make_order(rng, cfg, order_id, date, customer_ids, product_state):
    """Sinh mot don sach kem cac dong san pham (khong lap san pham)."""
    vol = cfg["volume"]
    n_items = rng.randint(*vol["items_per_order"])
    product_ids = sorted(rng.sample(sorted(product_state), n_items))
    items = []
    for pid in product_ids:
        quantity = rng.randint(*vol["quantity_range"])
        items.append({"order_id": order_id, "product_id": pid, "quantity": quantity,
                      "unit_price": product_state[pid]["list_price"]})
    order = {
        "order_id": order_id,
        "customer_id": rng.choice(customer_ids),
        "order_date": date,
        "total_amount": sum(i["quantity"] * i["unit_price"] for i in items),
        "status": weighted_choice(rng, cfg["status_weights"]),
        "channel": weighted_choice(rng, cfg["channel_weights"]),
        "updated_at": random_time(rng, date),
    }
    return {"order": order, "items": items}


def make_clean_orders(cfg, master):
    """Sinh don moi sach cho tung ngay, order_id tang dan tu ORD-1001.

    Tra ve dict date -> list {"order": {...}, "items": [...]}.
    Khach va gia san pham lay theo trang thai tich luy den ngay do.
    """
    rng = make_rng(cfg["seed"], "orders")
    customer_ids, product_state = [], {}
    next_id = 1001
    result = {}
    for date in cfg["dates"]:
        for c in master[date]["customers"]:
            if c["customer_id"] not in customer_ids:
                customer_ids.append(c["customer_id"])
        for p in master[date]["products"]:
            product_state[p["product_id"]] = p
        orders = []
        for _ in range(cfg["volume"]["orders_per_day"]):
            orders.append(make_order(rng, cfg, f"ORD-{next_id}", date,
                                     customer_ids, product_state))
            next_id += 1
        result[date] = orders
    return result


# ---------------------------------------------------------------------------
# Ban ghi minh hoa (tao thu cong, khong ngau nhien)
# ---------------------------------------------------------------------------
def make_showcase_orders(date):
    """Tra ve ORD-1001 va ORD-1002 dung nhu reading (gia tri sach)."""
    ord_1001 = {
        "order": {"order_id": "ORD-1001", "customer_id": "C-017", "order_date": date,
                  "total_amount": 150_000, "status": "delivered", "channel": "web",
                  "updated_at": f"{date} 09:15:00"},
        "items": [{"order_id": "ORD-1001", "product_id": "P-001", "quantity": 3,
                   "unit_price": 50_000}],
    }
    ord_1002 = {
        "order": {"order_id": "ORD-1002", "customer_id": "C-042", "order_date": date,
                  "total_amount": 150_000, "status": "shipping", "channel": "partner",
                  "updated_at": f"{date} 10:05:00"},
        "items": [{"order_id": "ORD-1002", "product_id": "P-002", "quantity": 2,
                   "unit_price": 75_000}],
    }
    return [ord_1001, ord_1002]


def format_ddmmyyyy(iso_date):
    """'2026-10-19' -> '19/10/2026' (loi A1)."""
    y, m, d = iso_date.split("-")
    return f"{d}/{m}/{y}"


def format_vnd(amount):
    """150000 -> '150.000đ' (loi A2)."""
    return f"{amount:,}".replace(",", ".") + "đ"


def apply_showcase(rows_orders):
    """Cai loi co dinh cho don minh hoa ngay 1.

    ORD-1001: xuat hien o ca web va app (B1), dong app co updated_at sau.
    ORD-1002: kenh partner, dinh C1 + A1 + A2.
    rows_orders: list LandingRow orders cua ngay 1 (da co ban goc).
    """
    by_id = {r.values["order_id"]: r for r in rows_orders}
    web_1001 = by_id["ORD-1001"]
    copy = LandingRow("orders", dict(web_1001.values), channel="app")
    copy.clean = dict(web_1001.clean)
    copy.values["channel"] = "app"
    copy.values["updated_at"] = web_1001.values["updated_at"].replace("09:15:00", "09:20:00")
    copy.add_error("B1_dup_cross_channel")
    rows_orders.append(copy)

    r = by_id["ORD-1002"]
    r.add_error("C1_missing_customer_id", customer_id="")
    r.add_error("A1_date_ddmmyyyy", order_date=format_ddmmyyyy(r.values["order_date"]))
    r.add_error("A2_amount_vnd_string", total_amount=format_vnd(r.values["total_amount"]))


# ---------------------------------------------------------------------------
# Lap bo du lieu (dong landing + loi) cho moi ngay
# ---------------------------------------------------------------------------
def build_dataset(cfg):
    """Sinh toan bo dong landing cho moi ngay, kem danh sach loi da cai.

    Tra ve dict date -> {"orders": [...], "order_items": [...],
    "customers": [...], "products": [...]} voi phan tu la LandingRow.
    """
    master = make_master_data(cfg)
    clean_orders = make_clean_orders(cfg, master)
    first_date = cfg["dates"][0]
    # thay hai don dau ngay 1 bang don minh hoa
    clean_orders[first_date][:2] = make_showcase_orders(first_date)

    dataset = {}
    for date in cfg["dates"]:
        day = {
            "products": [LandingRow("products", dict(p)) for p in master[date]["products"]],
            "customers": [LandingRow("customers", dict(c)) for c in master[date]["customers"]],
            "orders": [LandingRow("orders", dict(o["order"]), channel=o["order"]["channel"])
                       for o in clean_orders[date]],
            "order_items": [LandingRow("order_items", dict(i))
                            for o in clean_orders[date] for i in o["items"]],
        }
        dataset[date] = day
    apply_showcase(dataset[first_date]["orders"])
    return dataset


def main():
    """Diem vao dong lenh (hoan thien o cac task sau)."""
    cfg = load_config()
    dataset = build_dataset(cfg)
    for date, day in dataset.items():
        print(date, {entity: len(rows) for entity, rows in day.items()})


if __name__ == "__main__":
    main()
