# CLAUDE.md — Project: Bronze → Silver → Gold (Medallion Lakehouse cho dữ liệu bán hàng)

## 1. Bối cảnh

Repo này là phần **code + data + solution** cho bài giảng "Medallion Architecture" (AIO2026, khóa Self-Learning Data Engineering). Bài giảng gồm reading, slide, quiz, solution, code và data. Reading đã được viết riêng; repo này phải **khớp chính xác** với reading về tên bảng, tên cột, tên quy tắc và các con số minh họa (xem mục 9).

Mục tiêu sư phạm: học viên tự tay xây pipeline Bronze → Silver → Gold trên Delta Lake, thấy được
- data quality boundaries (quy tắc kiểm tra tại ranh giới giữa các lớp, chiến lược warn/drop/fail, bảng quarantine),
- reproducibility (Bronze bất biến, tính tất định, tính lũy đẳng, time travel, run log).

Yêu cầu bắt buộc của chương trình: bài học **tự chứa (self-contained)**. Toàn bộ dữ liệu được sinh trong repo, không dùng dữ liệu của buổi học khác, không gọi mạng khi chạy pipeline.

## 2. Nguyên tắc làm việc cho Claude Code

- Làm theo thứ tự các milestone ở mục 10. Hoàn thành và kiểm chứng xong milestone này mới sang milestone sau.
- Không tự ý thay đổi đặc tả (tên bảng, tên cột, tên quy tắc, quy tắc nghiệp vụ). Nếu thấy đặc tả mâu thuẫn hoặc không khả thi, dừng lại và báo cho người dùng kèm đề xuất.
- Code phải chạy được trên cả Windows, macOS và Linux. Dùng `pathlib`, không hard-code dấu phân cách đường dẫn.
- Mọi tính ngẫu nhiên phải dùng seed cố định từ `config.yaml`.
- Không dùng thời gian hệ thống cho logic nghiệp vụ. Ngày xử lý luôn được truyền vào qua tham số `--processing-date`. Thời gian hệ thống chỉ được dùng cho các cột metadata như `_ingest_time`, `_quarantined_at`, `run_started_at`.
- Identifier (tên biến, hàm, bảng, cột) bằng tiếng Anh. Comment và docstring bằng **tiếng Việt không dấu**, ngắn gọn, giống phong cách pseudocode trong reading.
- Ưu tiên code dễ đọc cho học viên hơn là tối ưu hiệu năng. Mỗi hàm làm một việc, có docstring.

## 3. Môi trường

- Python 3.10+.
- PySpark + `delta-spark`. **Tra bảng tương thích phiên bản trong docs Delta Lake**, chọn một cặp Spark / delta-spark tương thích, rồi ghim cứng cả hai trong `requirements.txt`. Ghi phiên bản Java cần thiết vào README.
- Tạo SparkSession bằng `configure_spark_with_delta_pip` cùng hai cấu hình bắt buộc của Delta (SQL extension và catalog). Đặt toàn bộ việc tạo SparkSession trong `common/spark.py`, để người dùng muốn chạy trên Databricks chỉ cần sửa một chỗ.
- Chạy ở chế độ `local[*]`. Đặt `spark.sql.shuffle.partitions` nhỏ (ví dụ 4) cho nhanh. Tắt log ồn ở mức WARN.
- Trên Windows, Spark cần `HADOOP_HOME` / `winutils.exe` để ghi file. Ghi hướng dẫn cài đặt vào README.
- Thư viện phụ: `pyyaml`, `pytest`. Không thêm thư viện khác nếu không thật sự cần.
- Bảng Delta lưu dạng **path-based** dưới `lakehouse/` (không cần metastore). Tên logic như `bronze.orders_raw` được ánh xạ sang đường dẫn `lakehouse/bronze/orders_raw` qua một hàm trong `common/paths.py`.

## 4. Cấu trúc repo

```
sales_lakehouse/
    CLAUDE.md
    README.md                   hướng dẫn cài đặt và chạy (tiếng Việt)
    requirements.txt
    config.yaml                 tham số chung (mục 5)
    generate_data.py            sinh dữ liệu nguồn + answer key
    common/
        spark.py                tạo SparkSession có Delta
        paths.py                ánh xạ tên bảng -> đường dẫn
        config.py               đọc config.yaml
        run_log.py              ghi/đọc run log và ingested_files
    quality_rules.py            khai báo quy tắc mức bản ghi (dạng dữ liệu)
    bronze_ingest.py            landing -> Bronze
    silver_transform.py         Bronze -> Silver + quarantine
    gold_build.py               Silver -> Gold
    quality_checks.py           kiểm tra mức tập dữ liệu
    run_pipeline.py             điều phối
    experiments/
        exp1_idempotency.py
        exp2_rule_change.py
        exp3_time_travel.py
    tests/
        test_against_answer_key.py
    tools/
        make_student_version.py sinh bản cho học viên (có TODO)
    landing/YYYY-MM-DD/         tệp nguồn từng ngày (sinh ra)
    answer_key/answer_key.json  đáp án (sinh ra)
    lakehouse/                  bronze/ silver/ gold/ quarantine/ _meta/ (sinh ra)
```

`landing/`, `answer_key/`, `lakehouse/` không commit kết quả chạy. Riêng bộ dữ liệu landing đã sinh sẵn sẽ được nén thành `data_landing.zip` để phát cho học viên (milestone 7).

## 5. config.yaml (giá trị mặc định đề xuất)

```yaml
seed: 42
dates: ["2026-10-19", "2026-10-20", "2026-10-21"]
volume:
  orders_per_day: 1500
  items_per_order: [1, 4]
  n_customers: 200
  n_products: 50
  categories: ["Electronics", "Fashion", "Home", "Beauty", "Sports", "Books"]
channels:
  web: csv
  app: csv
  partner: json
rules_version: v2            # v1 hoac v2, xem muc 7.3
quarantine_rate_threshold: 0.10
volume_drop_threshold: 0.5   # fail neu so don < 50% trung binh cac ngay truoc
enable_schema_evolution_day3: true
paths:
  landing: landing
  lakehouse: lakehouse
  answer_key: answer_key/answer_key.json
```

Tỷ lệ lỗi cài sẵn cũng đặt trong config (mục 6.3) để dễ chỉnh.

## 6. Đặc tả dữ liệu nguồn (generate_data.py)

### 6.1. Thực thể và cột (đúng tên, đúng thứ tự)

- `orders`: `order_id, customer_id, order_date, total_amount, status, channel, updated_at`
- `order_items`: `order_id, product_id, quantity, unit_price`
- `customers`: `customer_id, name, email, phone, city`
- `products`: `product_id, name, category, list_price`

Quy ước giá trị:
- `order_id` dạng `ORD-1001`, `ORD-1002`, ... tăng dần.
- `customer_id` dạng `C-001`...; `product_id` dạng `P-001`...
- Tiền tệ là VND, số nguyên.
- `status` hợp lệ: `pending, shipping, delivered, cancelled`.
- `updated_at` là timestamp ISO `YYYY-MM-DD HH:MM:SS`.
- Dữ liệu sạch mặc định: `order_date` dạng `YYYY-MM-DD`, `total_amount` là số, `total_amount` bằng tổng `quantity * unit_price` của các dòng.

### 6.2. Tệp đầu ra mỗi ngày

```
landing/YYYY-MM-DD/
    orders_web_YYYY-MM-DD.csv
    orders_app_YYYY-MM-DD.csv
    orders_partner_YYYY-MM-DD.json      JSON lines, phang (khong long nhau)
    order_items_YYYY-MM-DD.csv
    customers_YYYY-MM-DD.csv            ngay 1 la full snapshot, cac ngay sau chi la khach moi/cap nhat
    products_YYYY-MM-DD.csv             ngay 1 la full snapshot
```

Tên file đơn hàng phải trùng với ví dụ trong reading: `web_2026-10-19.csv`, `app_2026-10-19.csv`, `partner_2026-10-19.json` được dùng làm giá trị `_source_file` minh họa. Nếu muốn giữ đúng như reading thì đặt tên file theo dạng `<channel>_<date>.<ext>` trong thư mục `landing/<date>/orders/`. **Hãy chọn cách đặt tên sao cho `_source_file` của bản ghi ORD-1001/ORD-1002 hiển thị đúng như mục 9**, và ghi lại lựa chọn vào README.

### 6.3. Danh mục lỗi cài sẵn

Mỗi lỗi có một mã (`error_code`) để answer key đếm. Tỷ lệ là gợi ý mặc định, đặt trong config.

Nhóm A — chuẩn hóa được (vào Silver nếu rules_version = v2):
- `A1_date_ddmmyyyy`: `order_date` dạng `DD/MM/YYYY`. Khoảng 3% đơn, chủ yếu từ kênh partner.
- `A2_amount_vnd_string`: `total_amount` dạng chuỗi `"150.000đ"`. Khoảng 2% đơn.
- `A3_status_case`: `status` viết hoa lẫn lộn (`Delivered`, `DELIVERED`). Khoảng 10% đơn. Được chuẩn hóa ở cả v1 và v2.
- `A4_email_case`: email khách hàng khác nhau chỉ ở chữ hoa thường, cùng `customer_id` xuất hiện 2 dòng. Khoảng 5% khách.

Nhóm B — loại trùng:
- `B1_dup_cross_channel`: cùng `order_id` xuất hiện ở 2 kênh, `updated_at` khác nhau. Khoảng 2% đơn.
- `B2_dup_same_updated_at`: đúng 5 cặp trùng có `updated_at` giống hệt nhau, nhưng khác nội dung `status`. Dùng để bắt buộc có tie-breaker `_ingest_time`, `_source_file`.

Nhóm C — cách ly (quarantine):
- `C1_missing_customer_id`: `customer_id` rỗng. Khoảng 1% đơn.
- `C2_quantity_non_positive`: `quantity <= 0` trong order_items. Khoảng 1% dòng.
- `C3_unknown_product`: `product_id` không có trong products. Khoảng 1% dòng.

Nhóm W — chỉ cảnh báo:
- `W1_missing_phone`: khách thiếu `phone`. Khoảng 5% khách.
- `W2_amount_mismatch`: `total_amount` lệch so với tổng các dòng (lệch nhỏ, dưới 5%). Khoảng 2% đơn.

Nhóm T — theo thời gian:
- `T1_status_update_resend`: khoảng 10% đơn của ngày N được gửi lại trong tệp ngày N+1, cùng `order_id`, `updated_at` mới hơn, status tiến lên (`pending → shipping`, `shipping → delivered`). Dùng để minh họa MERGE cập nhật.
- `T2_late_arriving`: khoảng 3% đơn trong tệp ngày N+1 có `order_date` là ngày N. Dùng để minh họa Gold phải tính lại các ngày bị ảnh hưởng.
- `T3_schema_evolution` (nếu `enable_schema_evolution_day3`): tệp partner ngày 3 có thêm cột `promo_code`.

Ràng buộc khi cài lỗi:
- Một bản ghi có thể dính nhiều lỗi (ví dụ ORD-1002). Answer key phải đếm theo **bản ghi** và theo **mã lỗi** riêng biệt.
- Với rules_version = v1, tổng tỷ lệ cách ly mỗi ngày phải **dưới** `quarantine_rate_threshold`, để thí nghiệm 2 không bị pipeline fail.
- Đơn của khách hàng hợp lệ phải trỏ tới `customer_id` có thật.

### 6.4. Bản ghi minh họa bắt buộc (khớp reading)

Tạo thủ công, không ngẫu nhiên, trong ngày `2026-10-19`:

- **ORD-1001**: `customer_id = C-017`, `order_date = 2026-10-19`, `total_amount = 150000`, `status = delivered`. Xuất hiện ở **cả** file web và file app (lỗi B1), sao cho trong Bronze có hai dòng với `_source_file` lần lượt là file web và file app.
- **ORD-1002**: từ kênh partner, `customer_id` rỗng, `order_date = 19/10/2026`, `total_amount = "150.000đ"`, `status = shipping`. Dính đồng thời C1, A1, A2.

Order_items tương ứng phải nhất quán (tổng thành tiền của ORD-1001 = 150000).

### 6.5. Answer key (answer_key/answer_key.json)

Cho mỗi ngày và cho cả hai rules_version (v1, v2), ghi:
- Số bản ghi mỗi thực thể trong landing (= số dòng mong đợi trong Bronze).
- Số lần xuất hiện của từng `error_code`.
- Số bản ghi mong đợi: hợp lệ sau loại trùng, bị cách ly, trùng bị loại, cho mỗi thực thể.
- Danh sách `order_id` bị cách ly kèm danh sách quy tắc vi phạm mong đợi.
- Doanh thu mong đợi theo `order_date × category` và theo `order_date × channel`, tính sau khi xử lý xong **toàn bộ** các ngày đến ngày đó (đã áp dụng T1, T2).

Answer key được tính trực tiếp từ dữ liệu sạch trước khi cài lỗi cùng danh sách lỗi đã cài, **không** được tính bằng cách chạy lại chính pipeline (tránh "tự chấm điểm").

## 7. Đặc tả pipeline

### 7.1. Metadata và bảng hỗ trợ

- Cột metadata Bronze: `_ingest_time` (timestamp), `_source_file` (tên file, không kèm đường dẫn), `_batch_id` (dạng `b-0001`, tăng dần mỗi lần chạy bronze có nạp tệp mới).
- Ví dụ trong reading dùng `_batch_id = b-0042`. Không cần trùng số, nhưng định dạng phải giống.
- `lakehouse/_meta/ingested_files`: `file_name, file_path, entity, file_date, _batch_id, ingested_at, row_count`.
- `lakehouse/_meta/run_log`: một dòng mỗi lần chạy, các cột ở mục 7.6.

### 7.2. Bronze (bronze_ingest.py)

- Mỗi thực thể một bảng: `bronze.orders_raw`, `bronze.order_items_raw`, `bronze.customers_raw`, `bronze.products_raw`.
- Đọc **mọi cột dưới dạng string**. Gộp CSV và JSON của orders vào cùng một bảng, cột `channel` lấy từ dữ liệu.
- Kiểm tra mức tệp: tồn tại, không rỗng, parse được. Tệp lỗi thì fail lần chạy và ghi rõ tên tệp.
- Bỏ qua tệp đã có trong `ingested_files` (lũy đẳng ở mức tệp).
- Ghi bằng **append**. Cho phép schema evolution (`mergeSchema`) để nhận cột `promo_code` ngày 3.
- Bất biến: số dòng Bronze = tổng số dòng các tệp landing đã nạp.

### 7.3. Silver (silver_transform.py, quality_rules.py)

Thứ tự thực thể: `products → customers → orders → order_items`.

Trình tự cho mỗi thực thể:
1. **Chuẩn hóa**: ép kiểu, trim, lowercase `status` và `email`. Theo `rules_version`:
   - v1: chỉ parse `order_date` dạng `YYYY-MM-DD`; chỉ chấp nhận `total_amount` là số thuần.
   - v2: parse thêm `DD/MM/YYYY`; parse thêm chuỗi tiền dạng `"150.000đ"`.
2. **Áp quy tắc**: quy tắc khai báo dạng dữ liệu trong `quality_rules.py`, mỗi quy tắc gồm `name`, `entity`, `condition` (biểu thức cột Spark), `action` (`warn` | `drop`). Đánh giá **tất cả** quy tắc, không dừng ở quy tắc đầu tiên vi phạm.
3. **Tách nhánh**: bản ghi vi phạm ít nhất một quy tắc `drop` → quarantine. Quy tắc `warn` chỉ đếm vào run log.
4. **Loại trùng** trên tập hợp lệ: window theo khóa, sắp xếp `updated_at DESC, _ingest_time DESC, _source_file DESC`, giữ dòng đầu.
5. **MERGE** vào bảng Silver theo khóa. Update chỉ khi `updated_at` mới hơn hoặc bằng.

Tên quy tắc **bắt buộc** (khớp reading):
- `customer_id_not_null` (orders, drop)
- `order_date_valid_format` (orders, drop)
- `total_amount_numeric` (orders, drop)
- `status_in_allowed_set` (orders, drop)
- `quantity_positive` (order_items, drop)
- `product_exists` (order_items, drop)
- `order_exists` (order_items, drop): dòng thuộc đơn không có trong silver.orders
- `phone_not_null` (customers, warn)
- `total_amount_matches_items` (orders, warn): kiểm tra sau khi có order_items

Bảng Silver: `silver.products`, `silver.customers`, `silver.orders`, `silver.order_items` (thêm cột `line_amount = quantity * unit_price`).

Bảng quarantine (mỗi thực thể một bảng): `quarantine.orders`, `quarantine.order_items`, `quarantine.customers`, `quarantine.products`. Giữ nguyên các cột gốc (string) + `_failed_rules` (chuỗi các tên quy tắc nối bằng `"; "`), `_quarantined_at`, `_batch_id`. Trong reading, bảng quarantine của orders được gọi là `silver.orders_quarantine`; ánh xạ tên logic cho khớp.

Bảo toàn bản ghi, cho mỗi thực thể, mỗi lô:
`n_input = n_valid_after_dedup + n_quarantined + n_duplicates_removed`
(tính theo bản ghi đầu vào của lô, không theo số dòng bảng sau MERGE).

Hỗ trợ chế độ `--rebuild`: xóa Silver, quarantine, Gold rồi xử lý lại **mọi** batch theo thứ tự từ Bronze bằng `rules_version` hiện tại. Bronze không bị đụng tới.

### 7.4. Gold (gold_build.py)

Quy tắc nghiệp vụ:
- Doanh thu = tổng `line_amount` của các dòng thuộc đơn có `status != cancelled`.
- Doanh thu gán theo `order_date`, không theo ngày nạp.

Bảng:
- `gold.dim_customer`, `gold.dim_product` (có `category`), `gold.dim_date` (`date, day_of_week, month, quarter, year`). Ghi đè toàn bảng (bảng nhỏ).
- `gold.fact_sales`: grain = một dòng sản phẩm trong một đơn. Cột: `order_id, product_id, customer_id, order_date, channel, status, quantity, unit_price, line_amount`. Dùng khóa tự nhiên.
- `gold.daily_revenue_by_category`: `order_date, category, revenue, n_orders`.
- `gold.daily_revenue_by_channel`: `order_date, channel, revenue, n_orders`.
- `gold.top_customers`: `customer_id, total_revenue, n_orders, rank` trên toàn bộ dữ liệu.

Cách ghi fact và KPI theo ngày: **ghi đè theo phân vùng** (`replaceWhere`) cho **tất cả** các `order_date` bị ảnh hưởng bởi lô hiện tại (bao gồm ngày của đơn đến muộn và đơn được cập nhật trạng thái), không chỉ `processing_date`.

### 7.5. Kiểm tra mức tập dữ liệu (quality_checks.py)

Tất cả là `fail` khi vi phạm (raise exception, ghi run log với `status = failed`):
- `record_conservation`: đẳng thức bảo toàn ở mục 7.3, cho mỗi thực thể.
- `revenue_reconciliation`: doanh thu Gold theo ngày = doanh thu tính trực tiếp từ Silver.
- `no_orphan_keys`: mọi khóa trong `fact_sales` tồn tại trong các dim.
- `quarantine_rate`: tỷ lệ cách ly của lô < `quarantine_rate_threshold`.
- `volume_check`: số đơn của ngày ≥ `volume_drop_threshold` × trung bình các ngày trước (bỏ qua ở ngày đầu).

### 7.6. Run log

Cột: `run_id, _batch_id, processing_date, layer, rules_version, code_version, run_started_at, run_finished_at, status, input_files, n_input, n_valid, n_quarantined, n_duplicates_removed, n_warn_by_rule, delta_versions_written`.

- `code_version`: commit hash lấy bằng `git rev-parse HEAD`. Nếu không có git hoặc không trong repo, ghi `"unknown"`, không được lỗi.
- `n_warn_by_rule`, `delta_versions_written`, `input_files`: lưu dạng chuỗi JSON.

### 7.7. run_pipeline.py

```
python run_pipeline.py --processing-date 2026-10-19 --layer all
python run_pipeline.py --processing-date 2026-10-19 --layer bronze|silver|gold
python run_pipeline.py --rebuild --rules-version v2
python run_pipeline.py --all-dates          # chay lan luot moi ngay trong config
```

`--rules-version` ghi đè giá trị trong config. Mỗi layer chạy xong thì chạy các kiểm tra tương ứng ở 7.5 và ghi run log. Exit code khác 0 khi fail.

## 8. Thí nghiệm reproducibility (experiments/)

Mỗi script tự chạy pipeline với lakehouse sạch (thư mục tạm riêng), in kết quả dạng bảng ra console và **assert** kết quả mong đợi.

- `exp1_idempotency.py`: chạy ngày 2026-10-19 hai lần liên tiếp. Assert: số dòng Bronze, Silver, Gold và doanh thu không đổi; lần chạy thứ hai bỏ qua toàn bộ tệp.
- `exp2_rule_change.py`: chạy toàn bộ ngày với v1, ghi lại số cách ly và doanh thu. `--rebuild` với v2. Assert: số cách ly giảm đúng bằng số đơn chỉ dính A1/A2; doanh thu tăng đúng lượng trong answer key; ORD-1002 vẫn trong quarantine, `_failed_rules` của nó ở v1 là `customer_id_not_null; order_date_valid_format; total_amount_numeric`, ở v2 chỉ còn `customer_id_not_null`; Bronze không đổi.
- `exp3_time_travel.py`: in `DESCRIBE HISTORY` của silver.orders; đọc phiên bản trước và sau ngày 2 để chỉ ra các đơn T1 đổi trạng thái; cố ý ghi một lô sai vào silver.orders, rồi `RESTORE` về phiên bản trước và assert bảng khớp lại.

## 9. Ràng buộc khớp với reading

Reading đã dùng các giá trị sau; code và dữ liệu phải tạo ra đúng như vậy:
- Bronze `bronze.orders_raw` có ba dòng minh họa: ORD-1001 (web), ORD-1002 (partner), ORD-1001 (app), cùng một `_batch_id`, `_ingest_time` của dòng app sau dòng web.
- Sau Silver: ORD-1001 còn đúng một dòng, `order_date = 2026-10-19` kiểu date, `total_amount = 150000`, `status = delivered`.
- ORD-1002 trong quarantine với `_failed_rules` như mục 8 (theo v1) và các giá trị gốc `19/10/2026`, `150.000đ`.
- Tên bảng, tên quy tắc, tên cột metadata đúng như mục 7.

Sau milestone 6, xuất file `reading_numbers.md` liệt kê các con số thật (số dòng mỗi lớp mỗi ngày, số cách ly theo quy tắc, kết quả 3 thí nghiệm, trích `DESCRIBE HISTORY`) để tác giả điền vào mục Thực hành của reading.

## 10. Milestones

1. **Khung project**: cấu trúc thư mục, requirements đã ghim, `common/`, config, README cài đặt. Kiểm chứng: tạo được SparkSession, ghi và đọc lại một bảng Delta thử.
2. **generate_data.py + answer key**. Kiểm chứng: chạy hai lần ra file giống hệt (byte-by-byte); ORD-1001/ORD-1002 đúng mục 6.4; số lỗi khớp tỷ lệ cấu hình; tỷ lệ cách ly v1 dưới ngưỡng.
3. **Bronze**. Kiểm chứng: số dòng Bronze = số dòng landing; chạy lại không thêm dòng; ngày 3 nhận cột `promo_code`.
4. **Silver + quarantine**. Kiểm chứng: số hợp lệ / cách ly / trùng khớp answer key cho cả v1 và v2; đẳng thức bảo toàn đúng; tie-breaker cho kết quả ổn định qua nhiều lần chạy.
5. **Gold + quality_checks + run log**. Kiểm chứng: doanh thu theo `order_date × category` và `× channel` khớp answer key; đơn T2 làm thay đổi đúng ngày cũ.
6. **Experiments + tests**. `pytest` xanh; ba script thí nghiệm chạy qua; xuất `reading_numbers.md`.
7. **Bản cho học viên + đóng gói**:
   - Trong code solution, bao các phần cốt lõi bằng marker `# BEGIN SOLUTION` / `# END SOLUTION`: chuẩn hóa, điều kiện quy tắc, loại trùng, MERGE, cách ghi Gold, đối soát.
   - `tools/make_student_version.py` sinh thư mục `student/` thay mỗi khối bằng `# TODO: ...` kèm gợi ý ngắn (tiếng Việt không dấu) và `raise NotImplementedError`.
   - Nén `landing/` thành `data_landing.zip`.
   - README có phần "Cách tự chấm": chạy pipeline, chạy `pytest`, so với answer key.

## 11. Ngoài phạm vi

- Không viết reading, slide, quiz trong repo này (chỉ xuất `reading_numbers.md`).
- Không dùng streaming, Auto Loader, Unity Catalog, Lakeflow/DLT.
- Không dùng surrogate key, SCD type 2 (chỉ nhắc trong README như hướng mở rộng).
