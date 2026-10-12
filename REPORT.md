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

- `dataset_version` của bạn: `007d1c50120a`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `726 / 148 / 145` (tổng 1019 khung; 18 / 4 / 4 trip)
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

Có 1019 khung đáng dùng thuộc 26 trip. Chia **theo khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** nằm ở nhiều hơn một split, và cả **26 trip có khung ở cả train lẫn test**. Chia **theo trip** (gold của pipeline): **0 trip** bị chia cắt; test `no_straddle` trong dbt và verify "no trip is in two splits" đều PASS.

Hai khung liền nhau cách 0,5 s gần như giống hệt (cùng đường, cùng xe phía trước, cùng ánh sáng). Nếu khung t ở train và khung t+1 ở test, mô hình chỉ cần "nhớ" cảnh là đoán đúng: điểm test đo khả năng nhớ, không đo khả năng tổng quát hoá sang con đường mới. Khi triển khai trên đoạn đường chưa thấy, điểm thật sẽ thấp hơn nhiều so với con số báo cáo.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`export_dataset` lấy sha256 của danh sách `frame_id|split` đã sắp xếp, cắt 12 ký tự. Cùng nội dung thì cùng tên: chạy lại không đẻ ra "phiên bản mới" giả, hai người ở hai máy có thể so dataset bằng một chuỗi, và chỉ cần một khung đổi split là version đổi. Ngày giờ hay số đếm đổi mỗi lần chạy dù dữ liệu y hệt, nên không trả lời được "hai dataset này có giống nhau không".

Mình chạy `step 4` rồi `cli export-dataset` thêm **hai lần**: cả ba đều `007d1c50120a`, `manifest_sha256` cũng giống hệt (`49ef2dbe…`). DAG Airflow chạy từ lake trống và lần `step all --reset` cuối cùng cũng ra `007d1c50120a`.

MSSV được nối vào `md5(trip_id || LAB_STUDENT_ID)` để xếp thứ tự trip trong mỗi drive, nên cùng dữ liệu nhưng MSSV khác thì trip rơi vào split khác. Mình thử `LAB_STUDENT_ID=2024000` (giá trị mẫu trong `.env.example`): version thành `8fc131884584`, split 703 / 182 / 134 thay vì 726 / 148 / 145, số trip vẫn 18 / 4 / 4. Đặt lại MSSV của mình thì quay về `007d1c50120a`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/007d1c50120a/manifest.json` (lần chạy DAG từ lake trống) ghi:

```json
"lineage": {"bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
            "silver.frames": 25, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
            "input_video_sha256": {"drive_a": "636ba1a98dfc…", "drive_b": "bfa62703f645…"}}
```

Tức là: dataset được tạo từ hai video có sha256 `636ba1a98dfc5ba6…` (drive_a) và `bfa62703f645ff77…` (drive_b), từ version 0 của hai bảng telemetry thô và bảng trips, version 1 của `bronze.video_files` (hai lần ghi, mỗi drive một lần), và version 25 của `silver.frames` (26 lần ghi, mỗi trip một partition). Manifest còn ghi luật lọc (`is_black, is_blurry, is_near_dup, not sync_ok`), cách chia (`per trip, stratified by drive, md5 order`), `by_split`, `manifest_sha256` và `manifest.parquet` liệt kê từng ảnh.

Để tái tạo sau 3 tháng cần: sha256 video đầu vào, version của từng bảng Delta (time travel về đúng version đó), commit Git của mã pipeline/dbt (ngưỡng trong `dbt_project.yml`) và MSSV dùng làm salt. Có đủ những thứ đó thì chạy lại phải ra cùng `007d1c50120a`; ra khác là biết ngay có gì đã đổi.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trước đó, run `green_1` (sau `day09_reset`) xanh toàn bộ 37 task instance (3 bronze + `trip_commands` + 26 `silver_frames` + 7 task sau), `dataset_version` `007d1c50120a` (`airflow_graph.png`). Đặt `LAB_FAIL_ONCE=trip-02134`, áp dụng lại compose, trigger `day09_reset` rồi `day09_video_pipeline` (run `crash_1`):

- **1 task đỏ**: `silver_frames` `map_index=5`, log ghi `RuntimeError: simulated crash while extracting trip-02134; run it again` (`fail_task_log.png`).
- **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` (`airflow_fail_graph.png`).
- 29 task vẫn **success**, gồm 25 trip còn lại: lỗi của một partition không làm hỏng partition khác.

Mình chỉ clear task đỏ cùng các task phía sau nó (tương đương nút "Clear task" với Downstream + Only failed, chạy bằng `airflow tasks clear day09_video_pipeline -t '^silver_frames$' --only-failed --downstream`). Như vậy 8 task instance chạy lại: lần thử 2 của `trip-02134` ghi `{"trip": "trip-02134", "frames": 60}`, 25 trip kia **không** chạy lại. Run `crash_1` chuyển xanh 37/37 (`airflow_retry_graph.png`). Không nhân đôi: `lake.silver_frames` có 1246 dòng = 1246 `frame_id` khác nhau (bằng số trước khi sập). `scripts/lab.sh verify`: **27/27**, `dataset_version` vẫn **`007d1c50120a`**, giống run `green_1`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ước lượng: một trip trung bình ~48 khung (~24 s ở 2 khung/giây); 2 drive (~11 phút video) đã cho 1246 khung và 26 task. 500 xe × vài giờ lái mỗi ngày là hàng triệu khung và hàng chục nghìn partition mỗi ngày.

1. **Giải mã video và tính đặc trưng trên một container** (`silver_frames`: FFmpeg + numpy, `max_active_tis_per_dagrun=1`, `PARALLELISM=2`, LocalExecutor). Ở đây riêng bước 3 cho 26 trip đã mất ~45 s; tăng hàng trăm lần thì không kịp trong một ngày. Thay bằng worker phân tán (KubernetesExecutor/Celery hoặc Spark/Ray), giải mã bằng GPU (NVDEC), và chỉ chạy partition mới theo ngày (backfill theo partition).
2. **DuckDB một file + dbt trên một máy** (`work/lakehouse.duckdb`, `silver_frame_telemetry` join toàn bảng). Một tiến trình, một file khoá, RAM của một máy: ASOF join trên hàng trăm triệu dòng telemetry sẽ hết bộ nhớ, và các task không ghi song song được. Thay bằng engine phân tán đọc thẳng Delta (Spark/Trino/Databricks SQL), mô hình dbt dạng incremental theo `drive_id`/ngày.
3. **Lưu ảnh từng file JPEG nhỏ + Delta ghi theo từng trip trên một MinIO**. Hàng triệu object nhỏ mỗi ngày làm chậm list/copy (`export_dataset` copy từng ảnh), log Delta phình vì mỗi trip một commit, và nhiều task ghi cùng bảng sẽ xung đột commit. Thay bằng object store có scale (S3/GCS), ghi ảnh thành shard lớn (WebDataset/tar hoặc Parquet chứa bytes), partition bảng theo ngày/xe, compaction (OPTIMIZE) định kỳ; warehouse Postgres cho dashboard cũng nên chuyển sang kho cột (BigQuery/ClickHouse).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
select split, lighting, count(*) as trips, sum(frames_kept) as frames_kept,
       round(avg(avg_speed_kmh)::numeric, 1) as avg_speed_kmh
from marts.mart_trip_summary where split is not null
group by 1, 2 order by 1, 2
```

Câu hỏi: train/val/test có cùng phân bố điều kiện ánh sáng không, tức test có đo được mô hình trong mọi điều kiện lái không? Kết quả (thẻ "Câu hỏi riêng" cuối dashboard):

| split | lighting | trips | frames_kept | avg_speed_kmh |
|---|---|---|---|---|
| test | Day | 4 | 145 | 42.3 |
| train | Day | 18 | 726 | 53.8 |
| val | Day | 4 | 148 | 51.3 |

Diễn giải: cả 26 trip đều gắn nhãn `Day`, nên tập test **không có** trip đêm nào và không thể đo mô hình lúc tối, dù drive B có 61 khung tối (đường hầm, bóng râm) được giữ lại. Test cũng chậm hơn train (42 so với 54 km/h; riêng drive_a test chỉ 11 km/h). Chia theo trip tránh được rò rỉ nhưng không bảo đảm các split cân bằng về điều kiện; với dữ liệu thật nên chia theo trip **có phân tầng** theo ánh sáng và dải tốc độ.

## Điểm cộng · Thí nghiệm riêng và câu hỏi Metabase thứ hai

**Thí nghiệm 1 · Đổi tỉ lệ split** (`--vars '{split_train: 0.5, split_val: 0.25}'` cho `gold_dataset_frames`, ghi đè biến dbt, không sửa `dbt_project.yml`):

| | train | val | test | `dataset_version` |
|---|---|---|---|---|
| Trước (0.70 / 0.15) | 726 khung, 18 trip | 148, 4 trip | 145, 4 trip | `007d1c50120a` |
| Sau (0.50 / 0.25) | 566, 14 trip | 228, 6 trip | 225, 6 trip | `dde35c1e246b` |

Tổng vẫn 1019 khung; mỗi drive chuyển 2 trip từ train sang val và test (13 trip/drive: 7/3/3 thay vì 9/2/2). Version đổi vì danh sách `frame_id|split` đổi. Chạy lại không có biến ghi đè thì quay về đúng `007d1c50120a`.

**Thí nghiệm 2 · Độ chịu lệch đồng bộ `sync_tol_ms`** (chạy lại `silver_frame_telemetry+`):

| `sync_tol_ms` | mất đồng bộ A / B | giữ lại A / B | khung trong gold |
|---|---|---|---|
| 50 | 272 / 328 | 242 / 296 | 538 |
| 100 | 137 / 187 | 345 / 417 | 762 |
| **150 (mặc định)** | **7 / 7** | **436 / 583** | **1019** |
| 500 | 6 / 7 | 437 / 583 | 1020 |

Độ trễ trung bình khung–telemetry là 66 ms (A) và 73 ms (B): telemetry lấy mẫu khoảng 10–13 Hz, nên ngưỡng nhỏ hơn chu kỳ lấy mẫu (50, 100 ms) bỏ oan gần nửa số khung tốt. Từ 150 lên 500 ms chỉ cứu thêm **1 khung**, vì 13 khung còn lại nằm trong các đoạn cảm biến mất tín hiệu 2,9–3,1 s; nới tiếp chỉ gắn tốc độ cũ vào khung. Vì vậy 150 ms là điểm hợp lý. Sau thí nghiệm mình chạy lại với giá trị mặc định (7 / 7, 436 / 583, `007d1c50120a`), rồi chạy `step all --reset` lần cuối để tạo `run_summary.json`.

**Câu hỏi Metabase thứ hai (vận hành): trip nào bị loại nhiều khung nhất?**

```sql
select trip_id, drive_id, lighting, conditions, avg_speed_kmh, frames, frames_kept,
       round((100.0 * (frames - frames_kept) / frames)::numeric, 1) as pct_dropped, split
from marts.mart_trip_summary order by pct_dropped desc, trip_id limit 10
```

Top 4: `trip-04622` (drive_a, 67.1 km/h, 26/60 giữ lại, **56.7%** bị loại), `trip-03904` (46.1 km/h, 50.0%), `trip-02086` (17.6 km/h, 46.7%), `trip-08888` (6.5 km/h, 40.0%, nằm trong test). 8 trong 10 trip bị loại nhiều nhất thuộc drive_a, khớp funnel của drive_a (149 khung trùng, 6 đen, 9 mờ). Các trip chậm (`trip-08888`, `trip-07774` 12.8 km/h) bị loại vì khung trùng, đúng như Câu 5. Nhưng `trip-04622` chạy nhanh mà vẫn mất 57%, nên nhiều khả năng chứa đoạn camera đứng hình 6 s hoặc các khung đen/mờ của drive_a. Marts hiện chỉ có lý do loại theo **drive** nên không xác nhận được. Đề xuất vận hành: thêm `mart_trip_funnel` (lý do loại theo từng trip) và cảnh báo khi một trip mất > 40% khung, để biết ngay chuyến nào cần kiểm tra camera.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Bất ngờ nhất: chia ngẫu nhiên theo khung hình làm **cả 26/26 trip** rò rỉ sang test, không phải "một vài trip". Bất ngờ thứ hai: chỉ đổi MSSV đã đổi `dataset_version` và đổi phân bố tốc độ của test (drive_a test 11 km/h với MSSV của mình, 22 km/h với `2024000`). Với 26 trip, việc trip nào rơi vào test ảnh hưởng điểm mạnh hơn mô hình.
Chưa rõ: khi nào nên tách split theo cả địa điểm (cùng con đường đi nhiều lần), chứ không chỉ theo trip.
Lỗi pipeline chưa bắt: (1) cả 26 trip mang nhãn `lighting = Day` (lấy từ metadata trip), trong khi 61 khung ở drive B thực tế tối; không test nào đối chiếu nhãn với độ sáng đo được, nên nhãn điều kiện có thể sai mà không ai biết. (2) Độ lệch đồng hồ của B là hằng số khai báo tay (1500 ms); nếu đồng hồ trôi dần trong chuyến, ASOF join vẫn "khớp" trong 150 ms nhưng ghép sai mẫu, và không test nào phát hiện.
