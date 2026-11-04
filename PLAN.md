# PLAN.md

## Mục tiêu cuối cùng
Bàn giao bộ code, data và solution cho bài "Medallion Architecture": pipeline Bronze → Silver → Gold trên Delta Lake, chạy tất định và lũy đẳng, có quarantine, run log, 3 thí nghiệm reproducibility và bản cho học viên. Mọi tên và con số phải khớp `SPEC.md` mục 9.

## Định nghĩa "hoàn thành"
Project hoàn thành khi tất cả các điều sau đúng:
1. Mọi task bên dưới đã đánh dấu `[x]`.
2. `pytest tests/ -v` xanh toàn bộ.
3. `python run_pipeline.py --all-dates` chạy trên lakehouse sạch với exit code 0, và `pytest tests/test_against_answer_key.py` pass cho cả v1 và v2.
4. `python experiments/exp1_idempotency.py`, `exp2_rule_change.py`, `exp3_time_travel.py` đều chạy qua với exit code 0.
5. Có đủ `reading_numbers.md`, `data_landing.zip`, `student/` và README có phần "Cách tự chấm".

## Giả định đang dùng
- Thư mục gốc là thư mục hiện tại, không tạo thêm `sales_lakehouse/`.
- `customers`, `products`, `order_items` không có cột `updated_at`. Với các thực thể này, loại trùng sắp theo `_ingest_time DESC, _source_file DESC`, MERGE theo lô mới nhất (xem T33, T34).
- Khóa `order_items` là `(order_id, product_id)`, nên một đơn không có hai dòng cùng sản phẩm (xem T08).

## Cách đọc
- Làm theo thứ tự số. Xong một task thì chạy lệnh ở dòng "Xong khi", pass mới commit.
- Mỗi task tự viết test nêu trong "Xong khi". Lệnh `pytest` dùng lakehouse tạm, không đụng `lakehouse/` thật.

## Nhóm có thể làm song song
Chỉ gom các task khác file và không phụ thuộc nhau. Các task sửa chung một file thì làm tuần tự.

| Nhóm | Task | Điều kiện bắt đầu |
|---|---|---|
| P1 | T04, T05 | Xong T03 |
| P2 | Chuỗi T20–T21, chuỗi T22–T23 | Xong M2 |
| P3 | T27, chuỗi T28–T31 | Xong M3 |
| P4 | Chuỗi T41–T47, chuỗi T48–T50 | Xong M4 |
| P5 | T55, T56, T57 | Xong T54 |
| P6 | T61, T62, chuỗi T59–T60 | Xong M6 (T59–T60 cần code solution ổn định) |

---

## M1. Khung project

- [x] **T01. Khởi tạo git**
  - Việc: `git init`, tạo `.gitignore` bỏ qua `.venv/`, `landing/`, `answer_key/`, `lakehouse/`, `student/`, `__pycache__/`; commit đầu tiên gồm `CLAUDE.md`, `SPEC.md`, `PLAN.md`.
  - File: `.gitignore`
  - Xong khi: `git rev-parse HEAD` in ra hash và `git check-ignore landing lakehouse answer_key .venv` in đủ 4 dòng.

- [x] **T02. Ghim phụ thuộc**
  - Việc: tạo `requirements.txt` với 4 phiên bản ghim ở `CLAUDE.md`; tạo `pytest.ini` với `pythonpath = .`, `testpaths = tests`.
  - File: `requirements.txt`, `pytest.ini`
  - Xong khi: `pip install -r requirements.txt && pip check` thành công.

- [x] **T03. Config**
  - Việc: tạo `config.yaml` theo SPEC mục 5, thêm tỷ lệ lỗi mục 6.3; `common/config.py` đọc YAML và cho phép ghi đè `paths.lakehouse`, `paths.landing` (để test và thí nghiệm dùng thư mục tạm).
  - File: `config.yaml`, `common/__init__.py`, `common/config.py`, `tests/test_config.py`
  - Xong khi: `pytest tests/test_config.py` pass.

- [x] **T04. Ánh xạ tên bảng** (P1)
  - Việc: hàm đổi tên logic sang đường dẫn: `bronze.orders_raw` → `lakehouse/bronze/orders_raw`, `silver.orders_quarantine` → `lakehouse/quarantine/orders`. Dùng `pathlib`.
  - File: `common/paths.py`, `tests/test_paths.py`
  - Xong khi: `pytest tests/test_paths.py` pass.

- [x] **T05. SparkSession có Delta** (P1)
  - Việc: `common/spark.py` với `configure_spark_with_delta_pip`, 2 cấu hình Delta, `spark.sql.shuffle.partitions=4`, log WARN, `spark.jars.excludes` theo `CLAUDE.md`; fixture `spark` mức session trong `tests/conftest.py`.
  - File: `common/spark.py`, `tests/conftest.py`, `tests/test_spark.py`
  - Xong khi: `pytest tests/test_spark.py` pass (ghi rồi đọc lại một bảng Delta tạm).

- [x] **T06. README phần cài đặt**
  - Việc: viết hướng dẫn tiếng Việt: Python, Java, cài trên Linux/macOS, `HADOOP_HOME` và `winutils.exe` cho Windows, cách đặt tên file landing.
  - File: `README.md`
  - Xong khi: `grep -c -E "Java|HADOOP_HOME|winutils" README.md` ra ít nhất 3.

## M2. Dữ liệu nguồn và answer key
Toàn bộ M2 sửa chung `generate_data.py` và các lỗi tương tác nhau, nên làm tuần tự.

- [x] **T07. Sinh customers và products sạch**
  - Việc: sinh theo seed đúng cột SPEC 6.1, ID dạng `C-001`, `P-001`.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k master` pass.

- [x] **T08. Sinh orders và order_items sạch**
  - Việc: sinh đơn theo ngày, `order_id` tăng dần từ `ORD-1001`; `total_amount` = tổng `quantity * unit_price`; không lặp sản phẩm trong một đơn.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k clean_orders` pass.

- [x] **T09. Bản ghi minh họa**
  - Việc: tạo thủ công ORD-1001 và ORD-1002 theo SPEC 6.4, kèm order_items nhất quán. Lưu bản sạch cùng danh sách lỗi đã cài để answer key dùng.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k showcase` pass.

- [x] **T10. Cài lỗi nhóm A**
  - Việc: cài A1, A2, A3, A4 theo tỷ lệ config; A1 chủ yếu ở kênh partner.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k group_a` pass.

- [x] **T11. Cài lỗi nhóm B**
  - Việc: cài B1; cài đúng 5 cặp B2 cùng `updated_at`, khác `status`, hai dòng phải ở hai tệp khác nhau để tie-breaker có tác dụng.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k group_b` pass.

- [x] **T12. Cài lỗi nhóm C**
  - Việc: cài C1, C2, C3 theo tỷ lệ config.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k group_c` pass.

- [x] **T13. Cài lỗi nhóm W**
  - Việc: cài W1, W2 (lệch dưới 5%). Tách W2 khỏi các đơn bị lệch tổng tiền do C2/C3.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k group_w` pass.

- [x] **T14. Cài lỗi nhóm T**
  - Việc: cài T1 (gửi lại ngày N+1, status tiến lên), T2 (đến muộn), T3 (`promo_code` ở tệp partner ngày 3).
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k group_t` pass.

- [x] **T15. Ghi tệp landing**
  - Việc: ghi CSV và JSON lines theo SPEC 6.2, orders ở `landing/<date>/orders/<channel>_<date>.<ext>`; thêm `--output-dir`.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `python generate_data.py` tạo đủ tệp 3 ngày và `pytest tests/test_generate_data.py -k landing_files` pass.

- [x] **T16. Kiểm tra tất định**
  - Việc: bảo đảm thứ tự ghi và định dạng ổn định qua các lần chạy.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k deterministic` pass (hai lần chạy giống hệt từng byte).

- [x] **T17. Answer key: số bản ghi và mã lỗi**
  - Việc: ghi số dòng landing mỗi thực thể và số lần xuất hiện từng `error_code`, tính từ dữ liệu sạch và danh sách lỗi, không gọi pipeline.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k key_counts` pass.

- [x] **T18. Answer key: kết quả v1 và v2**
  - Việc: ghi số hợp lệ, cách ly, trùng bị loại cho mỗi thực thể, mỗi ngày; danh sách `order_id` cách ly kèm quy tắc vi phạm.
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py -k key_outcomes` pass.

- [x] **T19. Answer key: doanh thu và ngưỡng cách ly**
  - Việc: ghi doanh thu `order_date × category`, `order_date × channel` theo trạng thái lũy kế từng ngày; kiểm tra tỷ lệ cách ly v1 dưới ngưỡng cho cả orders và order_items (gồm `order_exists`).
  - File: `generate_data.py`, `tests/test_generate_data.py`
  - Xong khi: `pytest tests/test_generate_data.py` pass toàn bộ.

## M3. Bronze

- [x] **T20. Bảng ingested_files** (P2)
  - Việc: đọc và ghi `lakehouse/_meta/ingested_files`.
  - File: `common/run_log.py`, `tests/test_run_log.py`
  - Xong khi: `pytest tests/test_run_log.py -k ingested` pass.

- [x] **T21. Bảng run_log** (P2)
  - Việc: ghi và đọc đủ cột SPEC 7.6; `code_version` trả về `"unknown"` khi không có git.
  - File: `common/run_log.py`, `tests/test_run_log.py`
  - Xong khi: `pytest tests/test_run_log.py -k run_log` pass.

- [x] **T22. Tìm và kiểm tra tệp** (P2)
  - Việc: liệt kê tệp của một ngày theo thứ tự cố định (orders: web → app → partner); kiểm tra tồn tại, không rỗng, parse được; lỗi thì nêu rõ tên tệp.
  - File: `bronze_ingest.py`, `tests/test_bronze.py`
  - Xong khi: `pytest tests/test_bronze.py -k file_checks` pass.

- [x] **T23. Đọc tệp thành DataFrame string** (P2)
  - Việc: đọc CSV và JSON với mọi cột là string; thêm `_source_file`; `_ingest_time` tăng dần theo từng tệp để dòng app sau dòng web.
  - File: `bronze_ingest.py`, `tests/test_bronze.py`
  - Xong khi: `pytest tests/test_bronze.py -k read_as_string` pass.

- [x] **T24. Ghi Bronze**
  - Việc: append với `mergeSchema`, sinh `_batch_id` dạng `b-0001`, bỏ qua tệp đã nạp, cập nhật `ingested_files`. Cần T20.
  - File: `bronze_ingest.py`, `tests/test_bronze.py`
  - Xong khi: `pytest tests/test_bronze.py -k append_and_skip` pass (chạy lại không thêm dòng).

- [x] **T25. CLI run_pipeline cho Bronze**
  - Việc: dựng `run_pipeline.py` với `--processing-date`, `--layer bronze`, ghi run log, trả exit code khác 0 khi fail.
  - File: `run_pipeline.py`
  - Xong khi: `python run_pipeline.py --processing-date 2026-10-19 --layer bronze` trả exit code 0.

- [x] **T26. Kiểm chứng Bronze theo answer key**
  - Việc: so số dòng Bronze với landing cả 3 ngày; kiểm 3 dòng minh họa (SPEC mục 9); ngày 3 có `promo_code`.
  - File: `tests/test_bronze.py`
  - Xong khi: `pytest tests/test_bronze.py` pass toàn bộ.

## M4. Silver và quarantine

- [x] **T27. Khai báo quy tắc** (P3)
  - Việc: khai báo 9 quy tắc bắt buộc ở SPEC 7.3 dạng dữ liệu, đúng tên và `action`. Thứ tự orders phải cho ra `customer_id_not_null; order_date_valid_format; total_amount_numeric`.
  - File: `quality_rules.py`, `tests/test_quality_rules.py`
  - Xong khi: `pytest tests/test_quality_rules.py` pass.

- [x] **T28. Chuẩn hóa products và customers** (P3)
  - Việc: ép kiểu, trim, lowercase `email`.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k normalize_master` pass.

- [x] **T29. Chuẩn hóa orders theo v1** (P3)
  - Việc: lowercase `status`; chỉ parse `YYYY-MM-DD` và số thuần.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k normalize_orders_v1` pass.

- [x] **T30. Chuẩn hóa orders theo v2** (P3)
  - Việc: parse thêm `DD/MM/YYYY` và chuỗi `"150.000đ"`.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k normalize_orders_v2` pass.

- [x] **T31. Chuẩn hóa order_items** (P3)
  - Việc: ép kiểu, thêm `line_amount = quantity * unit_price`.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k normalize_items` pass.

- [x] **T32. Áp quy tắc và tách nhánh**
  - Việc: đánh giá tất cả quy tắc, tạo `_failed_rules`, tách hợp lệ và cách ly, đếm `warn`. Cần T27.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k apply_rules` pass.

- [x] **T33. Loại trùng**
  - Việc: window theo khóa với tie-breaker SPEC 7.3 bước 4; thực thể không có `updated_at` dùng phần còn lại.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k dedup` pass (gồm các cặp B2).

- [x] **T34. MERGE vào Silver**
  - Việc: MERGE theo khóa, orders chỉ update khi `updated_at` mới hơn hoặc bằng.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k merge` pass (T1 cập nhật, bản cũ hơn không ghi đè).

- [x] **T35. Ghi quarantine**
  - Việc: giữ cột gốc string, thêm `_failed_rules`, `_quarantined_at`, `_batch_id`.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k quarantine` pass.

- [x] **T36. Luồng Silver cho một lô**
  - Việc: nối các bước theo thứ tự `products → customers → orders → order_items`; lấy đúng dòng Bronze của lô chưa xử lý; trả thống kê `n_input`, `n_valid`, `n_quarantined`, `n_duplicates_removed`.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k batch_flow` pass.

- [x] **T37. Quy tắc total_amount_matches_items**
  - Việc: kiểm tra sau khi có order_items, chỉ đếm vào run log.
  - File: `silver_transform.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k amount_match` pass.

- [x] **T38. Nối Silver vào CLI**
  - Việc: thêm `--layer silver` và `--rules-version`, ghi run log.
  - File: `run_pipeline.py`
  - Xong khi: `python run_pipeline.py --processing-date 2026-10-19 --layer silver` trả exit code 0.

- [x] **T39. Chế độ rebuild**
  - Việc: xóa Silver, quarantine, Gold rồi xử lý lại mọi batch theo thứ tự; Bronze giữ nguyên.
  - File: `silver_transform.py`, `run_pipeline.py`, `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py -k rebuild` pass (phiên bản Bronze không đổi).

- [x] **T40. Kiểm chứng Silver theo answer key**
  - Việc: so số hợp lệ, cách ly, trùng cho v1 và v2; kiểm đẳng thức bảo toàn; chạy nhiều lần để xác nhận tie-breaker ổn định.
  - File: `tests/test_silver.py`
  - Xong khi: `pytest tests/test_silver.py` pass toàn bộ.

## M5. Gold, kiểm tra và run log

- [x] **T41. Các bảng dim** (P4)
  - Việc: ghi đè `gold.dim_customer`, `gold.dim_product`, `gold.dim_date`.
  - File: `gold_build.py`, `tests/test_gold.py`
  - Xong khi: `pytest tests/test_gold.py -k dims` pass.

- [x] **T42. Tìm các ngày bị ảnh hưởng** (P4)
  - Việc: lấy tập `order_date` chịu tác động của lô, gồm đơn đến muộn và đơn cập nhật trạng thái.
  - File: `gold_build.py`, `tests/test_gold.py`
  - Xong khi: `pytest tests/test_gold.py -k affected_dates` pass.

- [x] **T43. Bảng fact_sales** (P4)
  - Việc: dựng đúng grain và cột SPEC 7.4, ghi bằng `replaceWhere` theo các ngày bị ảnh hưởng.
  - File: `gold_build.py`, `tests/test_gold.py`
  - Xong khi: `pytest tests/test_gold.py -k fact_sales` pass.

- [x] **T44. KPI theo category** (P4)
  - Việc: `gold.daily_revenue_by_category`, loại đơn `cancelled`, ghi `replaceWhere`.
  - File: `gold_build.py`, `tests/test_gold.py`
  - Xong khi: `pytest tests/test_gold.py -k revenue_by_category` pass.

- [x] **T45. KPI theo channel** (P4)
  - Việc: `gold.daily_revenue_by_channel`, cùng cách ghi.
  - File: `gold_build.py`, `tests/test_gold.py`
  - Xong khi: `pytest tests/test_gold.py -k revenue_by_channel` pass.

- [x] **T46. Bảng top_customers** (P4)
  - Việc: `gold.top_customers` trên toàn bộ dữ liệu, có `rank`.
  - File: `gold_build.py`, `tests/test_gold.py`
  - Xong khi: `pytest tests/test_gold.py -k top_customers` pass.

- [x] **T47. Nối Gold vào CLI** (P4)
  - Việc: thêm `--layer gold`, ghi run log kèm `delta_versions_written`.
  - File: `run_pipeline.py`
  - Xong khi: `python run_pipeline.py --processing-date 2026-10-19 --layer gold` trả exit code 0.

- [x] **T48. Check record_conservation** (P4)
  - Việc: kiểm đẳng thức bảo toàn cho mỗi thực thể; vi phạm thì raise.
  - File: `quality_checks.py`, `tests/test_quality_checks.py`
  - Xong khi: `pytest tests/test_quality_checks.py -k conservation` pass.

- [x] **T49. Check quarantine_rate** (P4)
  - Việc: fail khi tỷ lệ cách ly của lô chạm `quarantine_rate_threshold`.
  - File: `quality_checks.py`, `tests/test_quality_checks.py`
  - Xong khi: `pytest tests/test_quality_checks.py -k quarantine_rate` pass.

- [x] **T50. Check volume_check** (P4)
  - Việc: so số đơn với `volume_drop_threshold` × trung bình các ngày trước; bỏ qua ngày đầu.
  - File: `quality_checks.py`, `tests/test_quality_checks.py`
  - Xong khi: `pytest tests/test_quality_checks.py -k volume` pass.

- [x] **T51. Check revenue_reconciliation**
  - Việc: so doanh thu Gold theo ngày với doanh thu tính trực tiếp từ Silver. Cần T44.
  - File: `quality_checks.py`, `tests/test_quality_checks.py`
  - Xong khi: `pytest tests/test_quality_checks.py -k reconciliation` pass.

- [ ] **T52. Check no_orphan_keys**
  - Việc: mọi khóa trong `fact_sales` phải có trong các dim. Cần T41, T43.
  - File: `quality_checks.py`, `tests/test_quality_checks.py`
  - Xong khi: `pytest tests/test_quality_checks.py -k orphan` pass.

- [ ] **T53. Nối check vào pipeline, thêm --all-dates**
  - Việc: chạy check sau từng layer; fail thì ghi run log `status = failed` và trả exit code khác 0; thêm `--all-dates`.
  - File: `run_pipeline.py`, `tests/test_quality_checks.py`
  - Xong khi: `python run_pipeline.py --all-dates` trả exit code 0 và `pytest tests/test_quality_checks.py -k pipeline_fails` pass.

## M6. Thí nghiệm, test tổng và số liệu

- [ ] **T54. Test tổng theo answer key**
  - Việc: chạy cả 3 ngày với v1 và v2 trên lakehouse tạm; so doanh thu 2 chiều; kiểm đơn T2 làm đổi đúng ngày cũ; kiểm giá trị SPEC mục 9.
  - File: `tests/test_against_answer_key.py`
  - Xong khi: `pytest tests/test_against_answer_key.py` pass.

- [ ] **T55. Thí nghiệm 1: lũy đẳng** (P5)
  - Việc: chạy ngày đầu hai lần; assert theo SPEC mục 8.
  - File: `experiments/exp1_idempotency.py`
  - Xong khi: `python experiments/exp1_idempotency.py` trả exit code 0.

- [ ] **T56. Thí nghiệm 2: đổi quy tắc** (P5)
  - Việc: chạy v1 rồi `--rebuild` v2; assert số cách ly, doanh thu, `_failed_rules` của ORD-1002, Bronze không đổi.
  - File: `experiments/exp2_rule_change.py`
  - Xong khi: `python experiments/exp2_rule_change.py` trả exit code 0.

- [ ] **T57. Thí nghiệm 3: time travel** (P5)
  - Việc: in `DESCRIBE HISTORY`, so phiên bản trước và sau ngày 2, ghi lô sai rồi `RESTORE`.
  - File: `experiments/exp3_time_travel.py`
  - Xong khi: `python experiments/exp3_time_travel.py` trả exit code 0.

- [ ] **T58. Xuất reading_numbers.md**
  - Việc: script gom số dòng mỗi lớp mỗi ngày, số cách ly theo quy tắc, kết quả 3 thí nghiệm, trích `DESCRIBE HISTORY`. Cần T55–T57.
  - File: `tools/export_reading_numbers.py`, `reading_numbers.md`
  - Xong khi: `python tools/export_reading_numbers.py` tạo file và `grep -c "DESCRIBE HISTORY" reading_numbers.md` ra ít nhất 1.

## M7. Bản cho học viên và đóng gói

- [ ] **T59. Thêm marker solution** (P6)
  - Việc: bao các phần cốt lõi bằng `# BEGIN SOLUTION` / `# END SOLUTION`: chuẩn hóa, điều kiện quy tắc, loại trùng, MERGE, cách ghi Gold, đối soát.
  - File: `quality_rules.py`, `silver_transform.py`, `gold_build.py`, `quality_checks.py`
  - Xong khi: `grep -c "BEGIN SOLUTION" silver_transform.py gold_build.py quality_rules.py quality_checks.py` mỗi file ra ít nhất 1 và `pytest tests/` vẫn pass.

- [ ] **T60. Sinh bản học viên** (P6)
  - Việc: `tools/make_student_version.py` tạo `student/`, thay mỗi khối bằng `# TODO: ...`, gợi ý tiếng Việt không dấu và `raise NotImplementedError`.
  - File: `tools/make_student_version.py`, `tests/test_student_version.py`
  - Xong khi: `pytest tests/test_student_version.py` pass (không còn marker, `python -m compileall student` thành công).

- [ ] **T61. Đóng gói dữ liệu** (P6)
  - Việc: nén `landing/` thành `data_landing.zip`.
  - File: `tools/package_data.py`, `data_landing.zip`
  - Xong khi: `python tools/package_data.py && python -m zipfile -l data_landing.zip` liệt kê đủ tệp 3 ngày.

- [ ] **T62. README phần tự chấm** (P6)
  - Việc: thêm "Cách tự chấm": chạy pipeline, chạy `pytest`, so answer key; nêu hướng mở rộng surrogate key, SCD type 2.
  - File: `README.md`
  - Xong khi: `grep -c "Cách tự chấm" README.md` ra ít nhất 1.

- [ ] **T63. Kiểm tra cuối**
  - Việc: chạy lại từ đầu trên bản clone sạch, đối chiếu từng mục ở "Định nghĩa hoàn thành".
  - File: không có file mới
  - Xong khi: `git clone . "$(mktemp -d)/final"`, cài theo `CLAUDE.md` trong bản clone, rồi `python generate_data.py && python run_pipeline.py --all-dates && pytest tests/ -v` đều pass.

---

## Vướng mắc
