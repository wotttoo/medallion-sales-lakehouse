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


def main():
    """Diem vao dong lenh (hoan thien o cac task sau)."""
    cfg = load_config()
    master = make_master_data(cfg)
    for date, data in master.items():
        print(date, len(data["products"]), "products,", len(data["customers"]), "customers")


if __name__ == "__main__":
    main()
