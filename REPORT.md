# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Hoàng Quốc Việt |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602563` |
| GitHub | `Catnip-harvest` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02134` |

Cách tính trip: MSSV của mình có chữ `A` nên `int("2A202602563")` trong lệnh của GUIDE báo lỗi. Mình bỏ ký tự không phải số và dùng `2202602563 % 26 = 5`; phần tử thứ 5 (đếm từ 0) của `scripts/lab.sh cli list-trips` là `trip-02134`, cũng là `map_index=5` của `silver_frames` trong Airflow.

Môi trường chạy: máy cá nhân (Windows) không có Docker, nên mình chạy đúng `docker-compose.yml` của đề trên một máy Ubuntu có Docker (GitHub Actions runner), không sửa mã của đề. Mọi lệnh và đầu ra lưu theo từng bước; ảnh chụp là giao diện thật của Airflow, MinIO và Metabase trên máy đó.

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt thấy trong 3 dòng đầu của `lake.bronze_raw_telemetry_a` và `_b`:

| | Nguồn A | Nguồn B |
|---|---|---|
| Thời gian | `ts_utc` chuỗi ISO-8601 (`2025-10-07T08:00:00.014Z`) | `timestamp_ms` số epoch mili-giây (`1759933801509`) |
| Tốc độ | `speed_mps` m/s (`1.213`) | `spd_kmh` km/h (`55.11`) |
| Bàn đạp | `gas`, `brake` tỉ lệ 0–1 (`0.028`) | `throttle_pct`, `brake_pct` phần trăm (`2.0`) |
| Tên cột | `heading_deg, lat, lon, alt_m, acc_x, acc_y, steer` | `hdg, latitude, longitude, altitude, ax, ay, steering` |
| Đồng hồ | lệch 0 ms | chạy **nhanh 1500 ms** so với video (`telemetry_clock_offset_ms = 1500` trong `bronze_video_files`) |

Bronze giữ nguyên bản để luôn có thể dựng lại silver khi luật làm sạch thay đổi: nếu ép kiểu ngay, ô trống ở `speed` (30 dòng mỗi drive) hay giá trị vô lý sẽ bị biến thành NULL/lỗi và mất dấu vết. Hai schema khác nhau nên gộp sớm buộc phải đoán ánh xạ ngay lúc nạp; tách hai bảng thì việc ánh xạ nằm ở một chỗ (staging dbt) và có test. Nạp lại lần hai báo `already present` (video nhận diện bằng sha256), `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, 3021 dòng), B cũng 1 commit (3234 dòng): chạy lại không nhân đôi.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

(trả lời ở đây)

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

(trả lời ở đây)

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
