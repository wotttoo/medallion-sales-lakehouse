# CLAUDE.md — Medallion Lakehouse cho dữ liệu bán hàng

## Tổng quan
Phần code, data và solution cho bài giảng "Medallion Architecture" (AIO2026, Self-Learning Data Engineering).
Học viên tự xây pipeline Bronze → Silver → Gold trên Delta Lake để thấy data quality boundaries và reproducibility.
Bài học tự chứa: dữ liệu sinh ngay trong repo, pipeline không gọi mạng khi chạy dữ liệu.

**Đặc tả đầy đủ nằm ở `SPEC.md`** (tên bảng, cột, quy tắc, tỷ lệ lỗi, milestones). Đọc mục liên quan trước khi code. Không tự đổi đặc tả; repo phải khớp reading.

## Tech stack
- Python 3.10+ (đã thử 3.13), Java 17+ (đã thử OpenJDK 21)
- `pyspark==4.1.1`, `delta-spark==4.3.1` (ghim cứng; delta-spark 4.3.1 yêu cầu `pyspark<=4.1.1`)
- `pyyaml==6.0.3`, `pytest==9.2.0`. Không thêm thư viện nếu không thật sự cần
- Chạy `local[*]`, bảng Delta path-based dưới `lakehouse/`, không dùng metastore

## Cài môi trường (Debian/Ubuntu sạch)
```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv openjdk-21-jdk-headless git
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Lần tạo SparkSession đầu tiên cần mạng để tải jar Delta từ Maven. `common/spark.py` phải đặt
`spark.jars.excludes=org.apache.hadoop:hadoop-client-api,org.apache.hadoop:hadoop-client-runtime`:
hai jar này đã có sẵn trong `pyspark`, nếu tải lại thì có thể treo trên mạng chậm.

## Chạy project và test
```bash
python generate_data.py                                    # sinh landing/ + answer_key/
python run_pipeline.py --all-dates                         # chạy lần lượt mọi ngày
python run_pipeline.py --processing-date 2026-10-19 --layer all   # hoặc bronze|silver|gold
python run_pipeline.py --rebuild --rules-version v2        # dựng lại từ Bronze
python experiments/exp1_idempotency.py                     # tương tự exp2, exp3
pytest tests/ -v
```

## Cấu trúc thư mục
```
SPEC.md, PLAN.md       đặc tả đầy đủ; tiến độ và mục "Vướng mắc"
config.yaml            seed, ngày, tỷ lệ lỗi, ngưỡng, đường dẫn
generate_data.py       sinh dữ liệu nguồn + answer key
common/                spark.py, paths.py, config.py, run_log.py
quality_rules.py       quy tắc mức bản ghi (khai báo dạng dữ liệu)
bronze_ingest.py       landing → Bronze
silver_transform.py    Bronze → Silver + quarantine
gold_build.py          Silver → Gold
quality_checks.py      kiểm tra mức tập dữ liệu
run_pipeline.py        điều phối
experiments/           3 thí nghiệm reproducibility
tests/                 đối chiếu với answer key
tools/                 sinh bản cho học viên
landing/ answer_key/ lakehouse/   sinh ra khi chạy, không commit
```

## Quy ước code
- Identifier bằng tiếng Anh; comment và docstring bằng tiếng Việt không dấu, ngắn gọn.
- Mỗi hàm làm một việc, có docstring. Ưu tiên dễ đọc cho học viên hơn tối ưu hiệu năng.
- Dùng `pathlib`, code chạy được trên Windows, macOS và Linux.
- Tính ngẫu nhiên dùng seed từ `config.yaml`. Ngày xử lý truyền qua `--processing-date`; giờ hệ thống chỉ dùng cho cột metadata.
- Tên logic của bảng ánh xạ sang đường dẫn qua `common/paths.py`; tạo SparkSession chỉ ở `common/spark.py`.
- Test: đặt ở `tests/test_*.py`, chạy trên thư mục lakehouse tạm. Giá trị mong đợi lấy từ answer key, không tính bằng chính pipeline.

## Quy tắc làm việc
- Làm theo thứ tự milestone. Mỗi task xong phải chạy test pass mới commit.
- Commit message rõ ràng: nói thay đổi gì và vì sao, mỗi commit một việc.
- Không xóa dữ liệu. Bronze và `landing/` bất biến; chỉ `--rebuild` được xóa Silver, quarantine, Gold đúng theo `SPEC.md`.
- Không hardcode API key hay secret trong code, config hay commit.
- Gặp vấn đề không tự giải quyết được: ghi vào mục "Vướng mắc" trong `PLAN.md` rồi chuyển sang task khác.
