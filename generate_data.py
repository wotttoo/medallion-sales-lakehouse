"""Sinh du lieu nguon (landing/) va answer key cho bai Medallion.

Moi tinh ngau nhien dung seed trong config.yaml, chay lai cho ra tep giong het.
Chay: python generate_data.py [--output-dir DIR]
"""

import argparse
import csv
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from common.config import load_config, resolve_path

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
# Loi dinh dang: ap khi ghi tep
# ---------------------------------------------------------------------------
def format_ddmmyyyy(iso_date):
    """'2026-10-19' -> '19/10/2026' (loi A1)."""
    y, m, d = iso_date.split("-")
    return f"{d}/{m}/{y}"


def format_vnd(amount):
    """150000 -> '150.000đ' (loi A2)."""
    return f"{amount:,}".replace(",", ".") + "đ"


# ma loi -> (cot, ham doi gia tri thanh chuoi loi)
FORMAT_ERRORS = {
    "A1_date_ddmmyyyy": ("order_date", format_ddmmyyyy),
    "A2_amount_vnd_string": ("total_amount", format_vnd),
    "C1_missing_customer_id": ("customer_id", lambda _v: ""),
}


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
    raw: dict = field(default_factory=dict)    # chuoi ghi de khi ghi tep
    clean: dict = None           # gia tri sach truoc khi cai loi
    tags: set = field(default_factory=set)     # nhan noi bo: new, resend, dup_copy, b_source

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
        """Doi dong thanh dict chuoi theo dung thu tu cot (dung khi ghi tep).

        Loi dinh dang (A1, A2, C1) duoc ap luc ghi, tu gia tri hien tai.
        """
        out = {}
        for col in columns:
            v = self.values.get(col)
            out[col] = "" if v is None else str(v)
        for code, (col, formatter) in FORMAT_ERRORS.items():
            if code in self.errors:
                out[col] = formatter(self.values[col])
        out.update({k: v for k, v in self.raw.items() if k in columns})
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
    copy.tags.add("dup_copy")
    web_1001.tags.add("b_source")
    rows_orders.append(copy)

    r = by_id["ORD-1002"]
    for code in ("C1_missing_customer_id", "A1_date_ddmmyyyy", "A2_amount_vnd_string"):
        r.add_error(code)


# ---------------------------------------------------------------------------
# Chon dong de cai loi
# ---------------------------------------------------------------------------
def random_error_pool(rows):
    """Don moi (khong phai don minh hoa, khong nam trong cap trung B) de cai loi."""
    return [r for r in rows
            if "new" in r.tags and "b_source" not in r.tags
            and r.values["order_id"] not in SHOWCASE_IDS]


def pick_rows(rng, pool, n):
    """Chon n dong tu pool (giu thu tu on dinh)."""
    n = max(0, min(n, len(pool)))
    return rng.sample(pool, n)


def n_new_orders(day):
    """So don moi trong ngay (khong tinh dong gui lai / dong trung)."""
    return sum(1 for r in day["orders"] if "new" in r.tags)


def already(rows, code):
    """So dong da co ma loi code (vd do ban ghi minh hoa)."""
    return sum(1 for r in rows if code in r.errors)


# ---------------------------------------------------------------------------
# Nhom A: chuan hoa duoc
# ---------------------------------------------------------------------------
def inject_group_a(cfg, date, day):
    """Cai A1 (chu yeu partner), A2, A3 vao orders va A4 vao customers."""
    rates = cfg["error_rates"]
    rng = make_rng(cfg["seed"], f"group_a:{date}")
    orders = day["orders"]
    n = n_new_orders(day)

    # A1: order_date dang DD/MM/YYYY, phan lon o kenh partner
    code = "A1_date_ddmmyyyy"
    target = count_for(rates[code], n)
    n_partner = int(target * rates["A1_partner_share"] + 0.5)
    pool = random_error_pool(orders)
    partner_pool = [r for r in pool if r.channel == "partner"]
    other_pool = [r for r in pool if r.channel != "partner"]
    done_partner = sum(1 for r in orders if code in r.errors and r.channel == "partner")
    done_other = already(orders, code) - done_partner
    for r in pick_rows(rng, partner_pool, n_partner - done_partner):
        r.add_error(code)
    for r in pick_rows(rng, other_pool, target - n_partner - done_other):
        r.add_error(code)

    # A2: total_amount dang chuoi "150.000đ"
    code = "A2_amount_vnd_string"
    pool = [r for r in random_error_pool(orders) if code not in r.errors]
    for r in pick_rows(rng, pool, count_for(rates[code], n) - already(orders, code)):
        r.add_error(code)

    # A3: status viet hoa lan lon
    code = "A3_status_case"
    pool = random_error_pool(orders)
    for r in pick_rows(rng, pool, count_for(rates[code], n)):
        style = rng.choice([str.title, str.upper])
        r.add_error(code, status=style(r.values["status"]))

    # A4: cung customer_id xuat hien 2 dong, email chi khac chu hoa thuong
    code = "A4_email_case"
    customers = day["customers"]
    for r in pick_rows(rng, list(customers), count_for(rates[code], len(customers))):
        copy = LandingRow("customers", dict(r.values))
        copy.clean = dict(r.clean)
        local, domain = r.values["email"].split("@")
        copy.add_error(code, email=f"{local.title()}@{domain.upper()}")
        copy.tags.add("dup_copy")
        customers.insert(customers.index(r) + 1, copy)


# ---------------------------------------------------------------------------
# Nhom B: loai trung
# ---------------------------------------------------------------------------
def shift_time(ts, minutes):
    """Cong/tru so phut vao chuoi 'YYYY-MM-DD HH:MM:SS'."""
    t = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=minutes)
    return t.strftime("%Y-%m-%d %H:%M:%S")


def make_dup_copy(source, channel, code):
    """Tao ban sao cua don o kenh khac, danh dau ma loi code."""
    copy = LandingRow("orders", dict(source.values), channel=channel)
    copy.clean = dict(source.clean)
    copy.values["channel"] = channel
    copy.add_error(code)
    copy.tags.add("dup_copy")
    source.tags.add("b_source")
    return copy


def inject_group_b(cfg, date, day):
    """Cai B1 (trung khac kenh, updated_at khac) va B2 (trung cung updated_at)."""
    rates = cfg["error_rates"]
    rng = make_rng(cfg["seed"], f"group_b:{date}")
    orders = day["orders"]
    channels = list(cfg["channels"])
    n = n_new_orders(day)

    def clean_pool():
        return [r for r in random_error_pool(orders) if not r.errors]

    # B1: cung order_id o 2 kenh, updated_at lech vai chuc phut (truoc hoac sau)
    code = "B1_dup_cross_channel"
    copies = []
    for src in pick_rows(rng, clean_pool(), count_for(rates[code], n) - already(orders, code)):
        channel = rng.choice([c for c in channels if c != src.channel])
        copy = make_dup_copy(src, channel, code)
        minutes = rng.choice([-1, 1]) * rng.randint(5, 60)
        copy.values["updated_at"] = shift_time(src.values["updated_at"], minutes)
        copies.append(copy)
    orders.extend(copies)

    # B2: dung N cap, updated_at giong het, status khac -> can tie-breaker
    code = "B2_dup_same_updated_at"
    copies = []
    for src in pick_rows(rng, clean_pool(), rates["B2_dup_same_updated_at_pairs"]):
        channel = rng.choice([c for c in channels if c != src.channel])
        copy = make_dup_copy(src, channel, code)
        copy.values["status"] = rng.choice(
            [s for s in cfg["status_weights"] if s != src.values["status"]])
        copies.append(copy)
    orders.extend(copies)


# ---------------------------------------------------------------------------
# Nhom C: cach ly
# ---------------------------------------------------------------------------
def inject_group_c(cfg, date, day):
    """Cai C1 (customer_id rong), C2 (quantity <= 0), C3 (san pham khong ton tai)."""
    rates = cfg["error_rates"]
    rng = make_rng(cfg["seed"], f"group_c:{date}")
    orders, items = day["orders"], day["order_items"]

    code = "C1_missing_customer_id"
    pool = [r for r in random_error_pool(orders) if code not in r.errors]
    for r in pick_rows(rng, pool, count_for(rates[code], n_new_orders(day)) - already(orders, code)):
        r.add_error(code)

    item_pool = [r for r in items if r.values["order_id"] not in SHOWCASE_IDS]
    n_items = len(items)

    code = "C2_quantity_non_positive"
    for r in pick_rows(rng, item_pool, count_for(rates[code], n_items)):
        r.values["quantity"] = rng.choice([0, -1, -2])
        r.add_error(code)

    code = "C3_unknown_product"
    pool = [r for r in item_pool if not r.errors]
    for r in pick_rows(rng, pool, count_for(rates[code], n_items)):
        r.values["product_id"] = f"P-{rng.randint(900, 999)}"
        r.add_error(code)


# ---------------------------------------------------------------------------
# Nhom W: chi canh bao
# ---------------------------------------------------------------------------
def inject_w1(cfg, dataset):
    """W1: khach thieu phone. Chon trong khach moi cua tung ngay; moi dong cua
    khach do (ke ca dong trung A4, dong cap nhat ngay sau) deu thieu phone."""
    rate = cfg["error_rates"]["W1_missing_phone"]
    missing = set()
    seen = set()
    for date in cfg["dates"]:
        rng = make_rng(cfg["seed"], f"group_w1:{date}")
        rows = dataset[date]["customers"]
        new_ids = sorted({r.values["customer_id"] for r in rows} - seen)
        seen |= set(new_ids)
        missing |= set(pick_rows(rng, new_ids, count_for(rate, len(new_ids))))
        for r in rows:
            if r.values["customer_id"] in missing:
                r.values["phone"] = None
                r.add_error("W1_missing_phone")


def deviate_amount(rng, total, max_deviation):
    """Lech total mot it (boi so 1000, khac 0, duoi max_deviation)."""
    pct = rng.uniform(0.01, max_deviation * 0.9) * rng.choice([-1, 1])
    delta = int(round(total * pct / 1000.0)) * 1000
    if delta == 0 or abs(delta) / total >= max_deviation:
        delta = 1000 if pct > 0 else -1000
    return total + delta


def inject_w2(cfg, date, day):
    """W2: total_amount lech nho so voi tong cac dong. Khong chon don da bi
    lech do C2/C3 (dong bi cach ly), de hai nguon lech khong chong nhau."""
    rates = cfg["error_rates"]
    rng = make_rng(cfg["seed"], f"group_w2:{date}")
    bad_item_orders = {r.values["order_id"] for r in day["order_items"]
                       if {"C2_quantity_non_positive", "C3_unknown_product"} & set(r.errors)}
    pool = [r for r in random_error_pool(day["orders"])
            if r.values["order_id"] not in bad_item_orders]
    code = "W2_amount_mismatch"
    for r in pick_rows(rng, pool, count_for(rates[code], n_new_orders(day))):
        r.values["total_amount"] = deviate_amount(rng, r.values["total_amount"],
                                                  rates["W2_max_deviation"])
        r.add_error(code)


# ---------------------------------------------------------------------------
# Nhom T: theo thoi gian
# ---------------------------------------------------------------------------
NEXT_STATUS = {"pending": "shipping", "shipping": "delivered"}
PROMO_CODES = ["SALE10", "FREESHIP", "VIP20"]


def inject_t2(cfg, prev_date, date, day):
    """T2: mot phan don moi cua ngay N+1 co order_date = N (den muon)."""
    rng = make_rng(cfg["seed"], f"group_t2:{date}")
    code = "T2_late_arriving"
    pool = random_error_pool(day["orders"])
    for r in pick_rows(rng, pool, count_for(cfg["error_rates"][code], n_new_orders(day))):
        r.values["order_date"] = prev_date
        r.values["updated_at"] = random_time(rng, prev_date, start_hour=21, end_hour=24)
        r.clean.update(order_date=r.values["order_date"], updated_at=r.values["updated_at"])
        r.add_error(code)
        r.tags.add("late")


def inject_t1(cfg, prev_date, date, dataset):
    """T1: gui lai mot phan don sach ngay N trong tep ngay N+1, status tien len,
    updated_at moi hon, cung kenh."""
    rng = make_rng(cfg["seed"], f"group_t1:{date}")
    code = "T1_status_update_resend"
    prev = dataset[prev_date]
    bad_item_orders = {r.values["order_id"] for r in prev["order_items"] if r.errors}
    pool = [r for r in random_error_pool(prev["orders"])
            if not r.errors and r.values["status"] in NEXT_STATUS
            and r.values["order_id"] not in bad_item_orders]
    resends = []
    for src in pick_rows(rng, pool, count_for(cfg["error_rates"][code], n_new_orders(prev))):
        row = LandingRow("orders", dict(src.values), channel=src.channel)
        row.values["status"] = NEXT_STATUS[src.values["status"]]
        row.values["updated_at"] = random_time(rng, date)
        row.clean = dict(row.values)
        row.add_error(code)
        row.tags.add("resend")
        resends.append(row)
    dataset[date]["orders"].extend(resends)


def inject_t3(cfg, dataset):
    """T3: tep partner ngay 3 co them cot promo_code."""
    if not cfg["enable_schema_evolution_day3"] or len(cfg["dates"]) < 3:
        return
    date = cfg["dates"][2]
    rng = make_rng(cfg["seed"], f"group_t3:{date}")
    for r in dataset[date]["orders"]:
        if r.channel == "partner":
            r.values["promo_code"] = rng.choice(PROMO_CODES) if rng.random() < 0.3 else None
            r.add_error("T3_schema_evolution")


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
        for r in day["orders"]:
            r.tags.add("new")
        dataset[date] = day
    apply_showcase(dataset[first_date]["orders"])

    dates = cfg["dates"]
    # T2 truoc tien: doi order_date truoc khi sao chep don cho nhom B
    for prev_date, date in zip(dates, dates[1:]):
        inject_t2(cfg, prev_date, date, dataset[date])
    for date in dates:
        inject_group_a(cfg, date, dataset[date])
        inject_group_b(cfg, date, dataset[date])
        inject_group_c(cfg, date, dataset[date])
        inject_w2(cfg, date, dataset[date])
    inject_w1(cfg, dataset)
    # T1 sau cung: chi gui lai don hoan toan sach cua ngay truoc
    for prev_date, date in zip(dates, dates[1:]):
        inject_t1(cfg, prev_date, date, dataset)
    inject_t3(cfg, dataset)
    return dataset


# ---------------------------------------------------------------------------
# Ghi tep landing
# ---------------------------------------------------------------------------
ENTITY_COLUMNS = {"orders": ORDER_COLUMNS, "order_items": ITEM_COLUMNS,
                  "customers": CUSTOMER_COLUMNS, "products": PRODUCT_COLUMNS}


def order_file_name(channel, date, fmt):
    """Ten tep don hang: <channel>_<date>.<ext> (khop _source_file trong reading)."""
    return f"{channel}_{date}.{fmt}"


def entity_file_name(entity, date):
    """Ten tep cua thuc the khac: <entity>_<date>.csv."""
    return f"{entity}_{date}.csv"


def order_sort_key(row):
    """Sap xep dong don theo so thu tu order_id roi updated_at."""
    return int(row.values["order_id"].split("-")[1]), row.values["updated_at"]


def write_csv(path, columns, raw_rows):
    """Ghi CSV UTF-8, xuong dong '\\n' de tep giong het tren moi he dieu hanh."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(columns)
        for raw in raw_rows:
            writer.writerow([raw[c] for c in columns])


def to_json_record(raw, columns):
    """Dong JSON phang: so tien thuan ghi dang so, cot rong cua promo_code la null."""
    rec = {}
    for c in columns:
        v = raw[c]
        if c == "total_amount" and v.isdigit():
            v = int(v)
        elif c == "promo_code" and v == "":
            v = None
        rec[c] = v
    return rec


def write_jsonl(path, columns, raw_rows):
    """Ghi JSON lines (moi dong mot object phang)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        for raw in raw_rows:
            f.write(json.dumps(to_json_record(raw, columns), ensure_ascii=False) + "\n")


def landing_files_for_day(cfg, date, day):
    """Tra ve list (duong dan tuong doi, dinh dang, cot, dong raw) cua mot ngay."""
    files = []
    for channel, fmt in cfg["channels"].items():
        rows = sorted((r for r in day["orders"] if r.channel == channel), key=order_sort_key)
        columns = list(ORDER_COLUMNS)
        if any("promo_code" in r.values for r in rows):
            columns.append("promo_code")
        rel = Path(date) / "orders" / order_file_name(channel, date, fmt)
        files.append((rel, fmt, columns, [r.to_raw(columns) for r in rows]))
    for entity in ["order_items", "customers", "products"]:
        columns = ENTITY_COLUMNS[entity]
        rel = Path(date) / entity_file_name(entity, date)
        files.append((rel, "csv", columns, [r.to_raw(columns) for r in day[entity]]))
    return files


def write_landing(cfg, dataset, landing_dir):
    """Ghi toan bo tep landing; tra ve list duong dan da ghi."""
    landing_dir = Path(landing_dir)
    written = []
    for date in cfg["dates"]:
        for rel, fmt, columns, raw_rows in landing_files_for_day(cfg, date, dataset[date]):
            path = landing_dir / rel
            if fmt == "json":
                write_jsonl(path, columns, raw_rows)
            else:
                write_csv(path, columns, raw_rows)
            written.append(path)
    return written


# ---------------------------------------------------------------------------
# Answer key: tinh tu du lieu sach + danh sach loi, KHONG chay pipeline
# ---------------------------------------------------------------------------
ERROR_CODES = [
    "A1_date_ddmmyyyy", "A2_amount_vnd_string", "A3_status_case", "A4_email_case",
    "B1_dup_cross_channel", "B2_dup_same_updated_at",
    "C1_missing_customer_id", "C2_quantity_non_positive", "C3_unknown_product",
    "W1_missing_phone", "W2_amount_mismatch",
    "T1_status_update_resend", "T2_late_arriving", "T3_schema_evolution",
]
ENTITIES = ["products", "customers", "orders", "order_items"]


def landing_counts(day):
    """So dong landing moi thuc the (= so dong mong doi trong Bronze)."""
    return {entity: len(day[entity]) for entity in ENTITIES}


def landing_file_counts(cfg, date, day):
    """So dong theo tung tep (ten tep = _source_file)."""
    return {rel.name: len(raw_rows)
            for rel, _fmt, _cols, raw_rows in landing_files_for_day(cfg, date, day)}


def error_counts(day):
    """So dong mang tung ma loi (mot dong co the mang nhieu ma)."""
    counts = {code: 0 for code in ERROR_CODES}
    for entity in ENTITIES:
        for r in day[entity]:
            for code in r.errors:
                counts[code] += 1
    return counts


# quy tac drop cua orders: ma loi -> ten quy tac, theo thu tu khai bao (SPEC 7.3)
ORDER_DROP_RULES = {
    "v1": [("C1_missing_customer_id", "customer_id_not_null"),
           ("A1_date_ddmmyyyy", "order_date_valid_format"),
           ("A2_amount_vnd_string", "total_amount_numeric")],
    "v2": [("C1_missing_customer_id", "customer_id_not_null")],
}
ENTITY_KEYS = {"products": ("product_id",), "customers": ("customer_id",),
               "orders": ("order_id",), "order_items": ("order_id", "product_id")}


def source_file_of(cfg, date, row):
    """Ten tep (= _source_file) chua dong landing."""
    if row.entity == "orders":
        return order_file_name(row.channel, date, cfg["channels"][row.channel])
    return entity_file_name(row.entity, date)


def row_key(row):
    """Khoa nghiep vu cua dong."""
    return tuple(row.values[c] for c in ENTITY_KEYS[row.entity])


class ExpectedLakehouse:
    """Mo hinh ket qua mong doi cua Silver, tinh tu gia tri sach + ma loi.

    Khong doc tep, khong parse chuoi: quyet dinh cach ly dua tren ma loi da cai
    (orders) hoac gia tri sau khi cai loi (order_items).
    """

    def __init__(self, cfg, version):
        """Khoi tao trang thai rong cho mot rules_version."""
        self.cfg = cfg
        self.version = version
        self.channel_rank = {ch: i for i, ch in enumerate(cfg["channels"])}
        self.state = {entity: {} for entity in ENTITIES}

    def failed_rules(self, row):
        """Danh sach quy tac drop ma dong vi pham (theo thu tu khai bao)."""
        if row.entity == "orders":
            return [rule for code, rule in ORDER_DROP_RULES[self.version] if code in row.errors]
        if row.entity == "order_items":
            failed = []
            if row.values["quantity"] <= 0:
                failed.append("quantity_positive")
            if (row.values["product_id"],) not in self.state["products"]:
                failed.append("product_exists")
            if (row.values["order_id"],) not in self.state["orders"]:
                failed.append("order_exists")
            return failed
        return []

    def order_rank(self, date, row):
        """Thu tu uu tien khi loai trung (lon hon = giu lai)."""
        file_name = source_file_of(self.cfg, date, row)
        if row.entity == "orders":
            return (row.values["updated_at"], self.channel_rank[row.channel], file_name)
        return (file_name,)

    def process_entity(self, date, entity, rows):
        """Ap quy tac, loai trung, merge mot thuc the; tra ve thong ke + ds cach ly."""
        valid, quarantined = [], []
        for row in rows:
            failed = self.failed_rules(row)
            if failed:
                quarantined.append((row, failed))
            else:
                valid.append(row)
        winners = {}
        for row in valid:
            k = row_key(row)
            if k not in winners or self.order_rank(date, row) > self.order_rank(date, winners[k]):
                winners[k] = row
        table = self.state[entity]
        for k, row in winners.items():
            old = table.get(k)
            if entity == "orders" and old is not None and row.values["updated_at"] < old["updated_at"]:
                continue
            table[k] = dict(row.values)
        stats = {"n_input": len(rows), "n_valid": len(winners),
                 "n_quarantined": len(quarantined),
                 "n_duplicates_removed": len(valid) - len(winners)}
        return stats, quarantined, list(winners)

    def items_total(self, order_id):
        """Tong line_amount cua cac dong Silver thuoc don."""
        return sum(v["quantity"] * v["unit_price"]
                   for (oid, _pid), v in self.state["order_items"].items() if oid == order_id)

    def process_day(self, date, day):
        """Xu ly mot lo (mot ngay) theo thu tu products -> customers -> orders -> order_items."""
        result = {"entities": {}, "quarantined": {}, "n_warn_by_rule": {}}
        order_keys = []
        for entity in ENTITIES:
            stats, quarantined, keys = self.process_entity(date, entity, day[entity])
            result["entities"][entity] = stats
            if entity == "orders":
                order_keys = keys
            if entity in ("orders", "order_items"):
                cols = ENTITY_KEYS[entity]
                result["quarantined"][entity] = sorted(
                    [dict({c: row.values[c] for c in cols},
                          source_file=source_file_of(self.cfg, date, row),
                          failed_rules="; ".join(failed))
                     for row, failed in quarantined],
                    key=lambda q: (int(q["order_id"].split("-")[1]),) + tuple(
                        q[c] for c in cols[1:]) + (q["source_file"],))
        result["n_warn_by_rule"]["phone_not_null"] = sum(
            1 for r in day["customers"] if not r.values["phone"])
        orders = self.state["orders"]
        result["n_warn_by_rule"]["total_amount_matches_items"] = sum(
            1 for (oid,) in order_keys if orders[(oid,)]["total_amount"] != self.items_total(oid))
        result["silver_counts"] = {entity: len(self.state[entity]) for entity in ENTITIES}
        result["quarantine_rate"] = {
            entity: (s["n_quarantined"] / s["n_input"] if s["n_input"] else 0.0)
            for entity, s in result["entities"].items()}
        return result


def build_answer_key(cfg, dataset):
    """Lap answer key cho moi ngay."""
    key = {"seed": cfg["seed"], "dates": list(cfg["dates"]), "days": {}}
    models = {v: ExpectedLakehouse(cfg, v) for v in ("v1", "v2")}
    for date in cfg["dates"]:
        day = dataset[date]
        key["days"][date] = {
            "landing_counts": landing_counts(day),
            "landing_file_counts": landing_file_counts(cfg, date, day),
            "error_counts": error_counts(day),
            "outcomes": {v: m.process_day(date, day) for v, m in models.items()},
        }
    return key


def write_answer_key(key, path):
    """Ghi answer key JSON (sap khoa, thut le co dinh de tat dinh)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(key, indent=2, sort_keys=True, ensure_ascii=False)
    path.write_text(text + "\n", encoding="utf-8", newline="")


def parse_args(argv=None):
    """Doc tham so dong lenh."""
    parser = argparse.ArgumentParser(description="Sinh du lieu landing va answer key")
    parser.add_argument("--output-dir", default=None,
                        help="thu muc goc de ghi landing/ va answer_key/ (mac dinh theo config)")
    return parser.parse_args(argv)


def main(argv=None):
    """Sinh du lieu, ghi landing va answer key ra dia."""
    args = parse_args(argv)
    cfg = load_config()
    if args.output_dir:
        landing_dir = Path(args.output_dir) / "landing"
        key_path = Path(args.output_dir) / "answer_key" / "answer_key.json"
    else:
        landing_dir = resolve_path(cfg, "landing")
        key_path = resolve_path(cfg, "answer_key")
    dataset = build_dataset(cfg)
    written = write_landing(cfg, dataset, landing_dir)
    key = build_answer_key(cfg, dataset)
    write_answer_key(key, key_path)
    print(f"Da ghi {len(written)} tep vao {landing_dir}")
    print(f"Da ghi answer key vao {key_path}")


if __name__ == "__main__":
    main()
