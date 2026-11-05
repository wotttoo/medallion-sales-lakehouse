# Medallion Lakehouse cho dữ liệu bán hàng

Phần code, data và solution cho bài "Medallion Architecture" (AIO2026, Self-Learning Data Engineering).
Bạn tự xây pipeline **Bronze → Silver → Gold** trên Delta Lake để thấy:

- **data quality boundaries**: quy tắc kiểm tra ở ranh giới giữa các lớp, chiến lược warn/drop/fail, bảng quarantine;
- **reproducibility**: Bronze bất biến, tính tất định, tính lũy đẳng, time travel, run log.

Bài học tự chứa: dữ liệu được sinh ngay trong repo, pipeline không gọi mạng khi xử lý dữ liệu.
Đặc tả đầy đủ nằm trong `SPEC.md`.

## 1. Yêu cầu

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Python | 3.10 trở lên (đã thử 3.13) | |
| Java (JDK) | 17 trở lên (đã thử OpenJDK 21) | Spark 4.x cần Java 17+ |
| pyspark | 4.1.1 | ghim trong `requirements.txt` |
| delta-spark | 4.3.1 | yêu cầu `pyspark<=4.1.1`, nên hai bản được ghim cùng nhau |
| pyyaml, pytest | 6.0.3, 9.2.0 | |

Kiểm tra Java bằng lệnh `java -version`. Nếu máy có nhiều bản Java, đặt `JAVA_HOME` trỏ tới bản 17+.

## 2. Cài đặt

### Linux (Debian/Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv openjdk-21-jdk-headless git
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### macOS

```bash
brew install python@3.13 openjdk@21
# them Java vao PATH (lam theo huong dan in ra sau khi brew cai xong), vi du:
export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows

1. Cài Python 3.10+ từ python.org (tick "Add python.exe to PATH").
2. Cài Java 17+ (ví dụ Eclipse Temurin 21) rồi đặt biến môi trường `JAVA_HOME`.
3. Spark trên Windows cần **`HADOOP_HOME`** và **`winutils.exe`** để ghi file:
   - tải `winutils.exe` và `hadoop.dll` đúng bản Hadoop 3.x (ví dụ từ repo `cdarlint/winutils` trên GitHub);
   - đặt vào `C:\hadoop\bin\`;
   - đặt `HADOOP_HOME=C:\hadoop` và thêm `%HADOOP_HOME%\bin` vào `PATH`;
   - mở terminal mới để biến môi trường có hiệu lực.
4. Tạo môi trường ảo:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Lần chạy đầu tiên cần mạng

Lần tạo SparkSession đầu tiên, Spark tải jar Delta từ Maven về thư mục `~/.ivy2`. Từ lần sau thì dùng lại bản đã tải.
`common/spark.py` đặt `spark.jars.excludes` để bỏ qua `hadoop-client-api` và `hadoop-client-runtime`.
Hai jar này đã có sẵn trong `pyspark`; nếu để Spark tải lại thì có thể bị treo trên mạng chậm.

Kiểm tra nhanh môi trường:

```bash
pytest tests/test_spark.py -v
```

## 3. Dữ liệu nguồn (landing) và cách đặt tên tệp

`python generate_data.py` sinh dữ liệu cho 3 ngày vào `landing/` và đáp án vào `answer_key/answer_key.json`:

```
landing/2026-10-19/
    orders/
        web_2026-10-19.csv
        app_2026-10-19.csv
        partner_2026-10-19.json     JSON lines, phang
    order_items_2026-10-19.csv
    customers_2026-10-19.csv        ngay 1: full snapshot; cac ngay sau: khach moi/cap nhat
    products_2026-10-19.csv         ngay 1: full snapshot; cac ngay sau: san pham doi gia
```

**Lựa chọn đặt tên:** tệp đơn hàng đặt theo dạng `landing/<date>/orders/<channel>_<date>.<ext>`.
Nhờ vậy cột `_source_file` (tên tệp, không kèm đường dẫn) của bản ghi minh họa hiển thị đúng như reading:
ORD-1001 có hai dòng với `_source_file = web_2026-10-19.csv` và `app_2026-10-19.csv`,
còn ORD-1002 có `_source_file = partner_2026-10-19.json`.
Các thực thể khác giữ tên `<entity>_<date>.csv` như SPEC mục 6.2.

## 4. Chạy pipeline

```bash
python generate_data.py                                         # sinh landing/ + answer_key/
python run_pipeline.py --all-dates                              # chay lan luot moi ngay
python run_pipeline.py --processing-date 2026-10-19 --layer all # hoac bronze | silver | gold
python run_pipeline.py --rebuild --rules-version v2             # dung lai Silver/Gold tu Bronze
```

Kết quả nằm trong `lakehouse/` (bảng Delta path-based: `bronze/`, `silver/`, `quarantine/`, `gold/`, `_meta/`).

Mỗi layer chạy xong sẽ chạy các kiểm tra mức tập dữ liệu (`quality_checks.py`) và ghi một dòng vào `lakehouse/_meta/run_log`.
Nếu kiểm tra fail, lần chạy ghi `status = failed`, các layer sau không chạy, và lệnh trả exit code 1.

| Layer | Kiểm tra |
|---|---|
| bronze | `volume_check` |
| silver | `record_conservation`, `quarantine_rate` |
| gold | `revenue_reconciliation`, `no_orphan_keys` |

## 5. Ba thí nghiệm reproducibility

Mỗi script tự sinh dữ liệu và chạy pipeline trong một thư mục tạm riêng, in bảng kết quả rồi `assert` (thêm `--keep` để giữ thư mục tạm):

```bash
python experiments/exp1_idempotency.py   # chay 2026-10-19 hai lan: khong doi gi, lan 2 bo qua moi tep
python experiments/exp2_rule_change.py   # v1 roi --rebuild v2: cach ly giam, doanh thu tang dung answer key
python experiments/exp3_time_travel.py   # DESCRIBE HISTORY, versionAsOf, ghi lo sai roi RESTORE
```

`python tools/export_reading_numbers.py` chạy lại tất cả và ghi các con số thật vào `reading_numbers.md`.

## 6. Bản cho học viên và dữ liệu đóng gói

```bash
python tools/make_student_version.py   # tao student/: moi khoi BEGIN/END SOLUTION -> TODO + NotImplementedError
python tools/package_data.py           # nen landing/ thanh data_landing.zip
```

Trong `student/`, các phần cốt lõi đã được thay bằng `# TODO` kèm gợi ý: chuẩn hóa (v2), điều kiện quy tắc, áp quy tắc,
loại trùng, MERGE, tìm ngày bị ảnh hưởng, cách ghi Gold (`replaceWhere`) và các kiểm tra đối soát.
`data_landing.zip` chứa đúng bộ dữ liệu mà `python generate_data.py` sinh ra (seed 42), giải nén ra `landing/`.

## 7. Cách tự chấm

1. Sinh dữ liệu và đáp án (hoặc giải nén `data_landing.zip`, nhưng vẫn cần chạy `generate_data.py` để có `answer_key/`):

   ```bash
   python generate_data.py
   ```

2. Chạy pipeline trên lakehouse sạch và kiểm tra exit code bằng 0:

   ```bash
   python run_pipeline.py --all-dates
   ```

   Xem các dòng `[silver] lo b-000N ...` và `[gold] check revenue_reconciliation ...` in ra.
   Đối chiếu với `answer_key/answer_key.json`:
   - `days.<ngay>.outcomes.v2.entities`: số hợp lệ / cách ly / trùng bị loại của từng thực thể;
   - `days.<ngay>.outcomes.v2.revenue_by_date`: doanh thu theo `order_date` sau khi xử lý xong ngày đó;
   - `rule_change`: số cách ly giảm và doanh thu tăng khi đổi từ v1 sang v2.

3. Chạy bộ test. Bộ test tự sinh dữ liệu và chạy pipeline trên thư mục tạm, nên không đụng `lakehouse/` của bạn.
   Giá trị mong đợi lấy từ answer key, không tính lại bằng pipeline.

   ```bash
   pytest tests/ -v                               # toan bo (khoang 15 phut)
   pytest tests/test_silver.py -k normalize -v    # tung phan khi dang lam TODO
   pytest tests/test_against_answer_key.py -v     # test tong: 3 ngay, ca v1 va v2
   ```

4. Chạy ba thí nghiệm ở mục 5: cả ba đều phải in `PASS`.

Answer key được tính trực tiếp từ dữ liệu sạch và danh sách lỗi đã cài trong `generate_data.py`
(lớp `ExpectedLakehouse`), không chạy lại pipeline. Nhờ vậy, khi test pass, pipeline của bạn đã ra cùng kết quả với một
cách tính độc lập.

## 8. Hướng mở rộng

- **Surrogate key**: bài này dùng khóa tự nhiên (`order_id`, `customer_id`, `product_id`) cho đơn giản. Trong kho dữ liệu
  thật, dim thường có khóa thay thế (ví dụ `customer_sk` sinh tăng dần hoặc bằng hash), fact tham chiếu tới khóa này.
  Cách này tách khỏi thay đổi khóa của hệ nguồn và hỗ trợ lưu lịch sử.
- **SCD type 2**: `gold.dim_customer` hiện ghi đè toàn bảng, nên khi khách đổi `city` thì giá trị cũ mất.
  Với SCD type 2, mỗi lần thay đổi sẽ thêm một dòng mới với `valid_from`, `valid_to`, `is_current`
  (dùng `MERGE` của Delta để đóng dòng cũ và chèn dòng mới). Fact khi đó gắn với phiên bản dim đúng tại thời điểm phát sinh đơn.
- Ngoài phạm vi bài: streaming / Auto Loader, Unity Catalog, Lakeflow/DLT.
