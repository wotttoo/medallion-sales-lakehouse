# Con so that cho muc Thuc hanh

File nay do `python tools/export_reading_numbers.py` sinh ra tu mot lan chay that (seed 42, du lieu trong repo). Chay lai se ra cung so dong/so cach ly/doanh thu; chi timestamp va `_ingest_time` khac.

## 1. So dong moi lop sau tung ngay (rules_version = v2)

| bang | 2026-10-19 | 2026-10-20 | 2026-10-21 |
|---|---|---|---|
| bronze.products_raw | 50 | 52 | 54 |
| bronze.customers_raw | 210 | 226 | 242 |
| bronze.orders_raw | 1535 | 3220 | 4905 |
| bronze.order_items_raw | 3700 | 7397 | 11189 |
| silver.products | 50 | 50 | 50 |
| silver.customers | 200 | 210 | 220 |
| silver.orders | 1485 | 2970 | 4455 |
| silver.order_items | 3589 | 7178 | 10854 |
| quarantine.products | 0 | 0 | 0 |
| quarantine.customers | 0 | 0 | 0 |
| quarantine.orders | 15 | 30 | 45 |
| quarantine.order_items | 111 | 219 | 335 |
| gold.dim_customer | 200 | 210 | 220 |
| gold.dim_product | 50 | 50 | 50 |
| gold.dim_date | 1 | 2 | 3 |
| gold.fact_sales | 3589 | 7178 | 10854 |
| gold.daily_revenue_by_category | 6 | 12 | 18 |
| gold.daily_revenue_by_channel | 3 | 6 | 9 |
| gold.top_customers | 200 | 210 | 220 |

## 2. Silver tung lo: hop le / cach ly / trung bi loai

### v1

| ngay | _batch_id | n_input | n_valid | n_quarantined | n_duplicates_removed | n_warn_by_rule |
|---|---|---|---|---|---|---|
| 2026-10-19 | b-0001 | 5495 | 5082 | 368 | 45 | {"phone_not_null": 10, "total_amount_matches_items": 93} |
| 2026-10-20 | b-0002 | 5400 | 4998 | 366 | 36 | {"phone_not_null": 1, "total_amount_matches_items": 95} |
| 2026-10-21 | b-0003 | 5495 | 5056 | 403 | 36 | {"phone_not_null": 1, "total_amount_matches_items": 99} |

### v2

| ngay | _batch_id | n_input | n_valid | n_quarantined | n_duplicates_removed | n_warn_by_rule |
|---|---|---|---|---|---|---|
| 2026-10-19 | b-0001 | 5495 | 5324 | 126 | 45 | {"phone_not_null": 10, "total_amount_matches_items": 99} |
| 2026-10-20 | b-0002 | 5400 | 5241 | 123 | 36 | {"phone_not_null": 1, "total_amount_matches_items": 101} |
| 2026-10-21 | b-0003 | 5495 | 5328 | 131 | 36 | {"phone_not_null": 1, "total_amount_matches_items": 106} |

## 3. So ban ghi cach ly theo quy tac (tong 3 ngay)

Mot ban ghi vi pham nhieu quy tac duoc dem o moi quy tac.

| thuc the | quy tac | v1 | v2 |
|---|---|---|---|
| order_items | order_exists | 664 | 112 |
| order_items | product_exists | 112 | 112 |
| order_items | quantity_positive | 112 | 112 |
| orders | customer_id_not_null | 45 | 45 |
| orders | order_date_valid_format | 135 | 0 |
| orders | total_amount_numeric | 90 | 0 |

## 4. Ban ghi minh hoa

### Bronze `bronze.orders_raw`

| order_id | customer_id | order_date | total_amount | status | _source_file | _batch_id | _ingest_time |
|---|---|---|---|---|---|---|---|
| ORD-1001 | C-017 | 2026-10-19 | 150000 | delivered | web_2026-10-19.csv | b-0001 | 2026-11-04 17:42:15.732153 |
| ORD-1001 | C-017 | 2026-10-19 | 150000 | delivered | app_2026-10-19.csv | b-0001 | 2026-11-04 17:42:16.064094 |
| ORD-1002 |  | 19/10/2026 | 150.000đ | shipping | partner_2026-10-19.json | b-0001 | 2026-11-04 17:42:16.360238 |

### Silver `silver.orders` (ORD-1001 con mot dong)

| order_id | customer_id | order_date | total_amount | status | channel | updated_at |
|---|---|---|---|---|---|---|
| ORD-1001 | C-017 | 2026-10-19 | 150000 | delivered | app | 2026-10-19 09:20:00 |

### Quarantine `silver.orders_quarantine` (v1)

| order_id | customer_id | order_date | total_amount | status | _failed_rules | _batch_id |
|---|---|---|---|---|---|---|
| ORD-1002 |  | 19/10/2026 | 150.000đ | shipping | customer_id_not_null; order_date_valid_format; total_amount_numeric | b-0001 |

## 5. Thi nghiem 1 - Luy dang (chay 2026-10-19 hai lan)

| bang | lan 1 | lan 2 |
|---|---|---|
| bronze.products_raw | 50 | 50 |
| bronze.customers_raw | 210 | 210 |
| bronze.orders_raw | 1535 | 1535 |
| bronze.order_items_raw | 3700 | 3700 |
| silver.products | 50 | 50 |
| silver.customers | 200 | 200 |
| silver.orders | 1485 | 1485 |
| silver.order_items | 3589 | 3589 |
| quarantine.products | 0 | 0 |
| quarantine.customers | 0 | 0 |
| quarantine.orders | 15 | 15 |
| quarantine.order_items | 111 | 111 |
| gold.dim_customer | 200 | 200 |
| gold.dim_product | 50 | 50 |
| gold.dim_date | 1 | 1 |
| gold.fact_sales | 3589 | 3589 |
| gold.daily_revenue_by_category | 6 | 6 |
| gold.daily_revenue_by_channel | 3 | 3 |
| gold.top_customers | 200 | 200 |

- Doanh thu lan 1: `{'2026-10-19': 5713250000}`, lan 2: `{'2026-10-19': 5713250000}`
- Tep nap o lan 2: `[]` (bo qua toan bo 6 tep)

## 6. Thi nghiem 2 - Doi quy tac v1 -> v2 (--rebuild)

| thuc the | cach ly v1 | cach ly v2 |
|---|---|---|
| products | 0 | 0 |
| customers | 0 | 0 |
| orders | 265 | 45 |
| order_items | 872 | 335 |

| order_date | doanh thu v1 | doanh thu v2 | tang |
|---|---|---|---|
| 2026-10-19 | 5588195000 | 5893280000 | 305085000 |
| 2026-10-20 | 5368760000 | 5643850000 | 275090000 |
| 2026-10-21 | 5386695000 | 5701150000 | 314455000 |

- So don cach ly giam: **220** (= so don chi dinh A1/A2)
- Doanh thu tang: **894630000** VND
- `_failed_rules` cua ORD-1002: v1 `customer_id_not_null; order_date_valid_format; total_amount_numeric`, v2 `customer_id_not_null`
- Bronze khong doi: phien ban `{'products': 2, 'customers': 2, 'orders': 2, 'order_items': 2}`, so dong `{'products': 54, 'customers': 242, 'orders': 4905, 'order_items': 11189}`

## 7. Thi nghiem 3 - Time travel

### DESCRIBE HISTORY silver.orders

| version | timestamp | operation | operationMetrics (rut gon) |
|---|---|---|---|
| 0 | 2026-11-04 17:50:15 | WRITE | {'numOutputRows': '0'} |
| 1 | 2026-11-04 17:50:16 | MERGE | {'numTargetRowsInserted': '1485', 'numTargetRowsUpdated': '0', 'numOutputRows': '1485'} |
| 2 | 2026-11-04 17:50:48 | MERGE | {'numTargetRowsInserted': '1485', 'numTargetRowsUpdated': '150', 'numOutputRows': '2970'} |
| 3 | 2026-11-04 17:51:20 | MERGE | {'numTargetRowsInserted': '1485', 'numTargetRowsUpdated': '150', 'numOutputRows': '4455'} |

- Phien ban sau ngay 1: `1`, sau ngay 2: `2`
- So don doi trang thai giua hai phien ban (T1): **150** `{'pending -> shipping': 83, 'shipping -> delivered': 67}`
- Lo sai co y ghi vao: 1441 dong; RESTORE ve version `3`, bang khop lai hoan toan

### DESCRIBE HISTORY sau RESTORE (3 dong cuoi)

| version | operation | operationMetrics (rut gon) |
|---|---|---|
| 3 | MERGE | {'numTargetRowsInserted': '1485', 'numTargetRowsUpdated': '150', 'numOutputRows': '4455'} |
| 4 | UPDATE | {} |
| 5 | RESTORE | {'numRestoredFiles': '1'} |

