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

`stg_telemetry_b.sql` đổi tên cột về schema của A, chia `spd_kmh / 3.6` để ra m/s, chia pedal `/ 100`, và tính `to_timestamp((timestamp_ms - offset_ms) / 1000.0)` với `offset_ms` lấy từ `bronze.video_files` (1500 ms cho drive_b). A chỉ ép kiểu (`ts_utc` → `timestamptz`, `nullif(speed_mps, '')`). Sau đó `silver_telemetry_classified` `union all by name` hai nguồn và gắn `reject_reason`.

Bằng chứng: dòng sạch đầu tiên của B là `2025-10-08 14:30:00.009`, khớp giờ bắt đầu video `14:30:00` (dòng thô là `1759933801509` = `14:30:01.509`, tức đã trừ đúng 1,5 s; verify: "first sample within 0.5 s of video start" PASS). Tốc độ tối đa sau chuẩn hoá: A 19.82 m/s (71 km/h), B 27.83 m/s (100 km/h), đều hợp lý.

| drive | duplicate | speed_missing | speed_out_of_range | sạch | sạch + cách ly = nhận |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 3181 | 3181 + 53 = 3234 |

Tổng `silver.telemetry` 6149 dòng, `silver.telemetry_rejects` 106 dòng. Dòng lỗi được giữ kèm lý do để đếm được (biết cảm biến nào hỏng, hỏng bao nhiêu), để kiểm toán "sạch + cách ly = nhận" và để xử lý lại nếu luật sai; xoá im lặng thì không ai biết dữ liệu đã mất.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống > 1000 ms giữa hai mẫu cùng drive) báo một khiếm khuyết **có thật của thế giới**: cảm biến mất tín hiệu vài giây (khoảng trống đồng bộ lớn nhất mình thấy là 2884 ms ở A, 3112 ms ở B). Dữ liệu vẫn đúng, chỉ thưa hơn, và các khung trong khoảng đó đã bị bỏ ở bước join (`sync_lost` 7 khung mỗi drive), nên chỉ cần cảnh báo (`PASS=6 WARN=1 ERROR=0`). `in_range` (0..70 m/s) bảo vệ một bất biến: tốc độ vô lý trong silver là dữ liệu **sai**, sẽ đi thẳng vào nhãn của dataset, nên phải chặn.

Thí nghiệm `break-gate` nới `max_speed_mps` từ 70 lên 1000: luật làm sạch không còn cách ly gì, nhưng `in_range_silver_telemetry_speed_mps__70__0` **FAIL, bắt được 6 dòng** (đúng 3 + 3 dòng `speed_out_of_range` trước đó bị cách ly), tóm tắt `PASS=5 WARN=1 ERROR=1`. Khôi phục luật: `PASS=6 WARN=1 ERROR=0`. Nghĩa là cổng chất lượng là lớp phòng thủ thứ hai, độc lập với luật làm sạch: khi ai đó sửa nhầm cấu hình, test error dừng DAG (trong Airflow `dbt_test_silver` đỏ thì gold không chạy) trước khi dữ liệu xấu tới gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Mình chạy lại partition `trip-02134` (trip của mình) hai lần: bảng `silver.frames` **1246 dòng trước, 1246 dòng sau**, dấu vân tay `6c7480dfde7e` → `6c7480dfde7e` (`unchanged: true`). Lịch sử Delta cho thấy hai commit mới (version 26 và 27), mỗi commit `WRITE` 60 dòng với predicate `trip_id = 'trip-02134'`: mỗi lần thay đúng 60 dòng của trip đó. Cùng 60 khung đó ghi kiểu `append` hai lần cho ra **120 dòng (gấp 2)**.

`overwrite` theo predicate là **một commit nguyên tử**: file mới được ghi xong rồi log Delta mới trỏ sang, đồng thời đánh dấu file cũ của trip là đã xoá. Nếu tiến trình chết giữa chừng thì commit không xảy ra, người đọc vẫn thấy phiên bản cũ đầy đủ, và chạy lại cho cùng kết quả. "Xoá rồi ghi" là hai thao tác: chết sau khi xoá và trước khi ghi xong thì bảng **mất** trip đó (hoặc có nửa trip) cho tới khi có người phát hiện; các trip khác chạy song song cũng có thể thấy trạng thái dở dang.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung **đen** có độ sáng trung bình < 12 (`dark_luma`): không có thông tin (camera che, lỗi cảm biến), bị bỏ; độ sáng lớn nhất của khung đen là 0.0. Khung **tối** có 12 ≤ độ sáng < 40 (`dark_soft`): đường hầm, bóng râm, chiều tối. Đó là điều kiện lái thật, nên chỉ gắn cờ `is_dark` và **giữ lại**. Drive A: 6 đen, 4 tối giữ lại; drive B: 4 đen, **61** tối giữ lại (khung tối mờ nhất của B có độ sáng 12.8, sát ngưỡng). Mình đồng ý: bỏ khung tối thì mô hình chỉ học được ban ngày.

Mình giữ ngưỡng **2**. Ngưỡng 0 bỏ sót camera đứng hình có nhiễu nén (A vẫn còn 149 − 61 = 88 khung gần trùng). Ngưỡng 6 loại 363/602 khung ở A (60%), tức loại cả khung khác nhau thật. Ngưỡng phụ thuộc tốc độ: lấy mẫu 2 khung/giây, xe đi 50 km/h di chuyển ~7 m giữa hai khung nên cảnh đổi nhiều; xe đi 5 km/h chỉ ~0,7 m nên hai khung liền nhau tự nhiên đã gần trùng. A chạy chậm hơn B nên ở mọi ngưỡng A đều có nhiều khung trùng hơn. Hệ thống lớn nên cho ngưỡng (hoặc tốc độ lấy mẫu) theo tốc độ xe thay vì một hằng số.

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
