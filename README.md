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
