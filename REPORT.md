# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Trần Thị Như Ý` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602372` |
| GitHub | `nhuY02` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02689` |

## Kết quả cuối

- `dataset_version` của bạn: `cd79f04deba6`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `734 / 115 / 170` (tổng cộng 1019 khung hình)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (`airflow_task_crash.png` thấy rõ task đỏ).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Các khác biệt chính giữa Telemetry A và Telemetry B được ghi nhận ở Bronze:
1. Định dạng timestamp: Nguồn A dùng chuỗi chuẩn ISO 8601 UTC (`ts_utc`, ví dụ `2025-10-07T08:00:00.014Z`), trong khi Nguồn B dùng số nguyên Unix timestamp epoch milliseconds (`timestamp_ms`, ví dụ `1759933801509`).
2. Đơn vị vận tốc: Nguồn A sử dụng đơn vị mét/giây (`speed_mps`), nguồn B sử dụng kilômét/giờ (`spd_kmh`).
3. Đơn vị và tên bàn đạp/góc lái: Nguồn A dùng `gas`, `brake` ở dạng tỷ lệ thập phân [0, 1] và `steer`; Nguồn B dùng `throttle_pct`, `brake_pct` theo phần trăm [0, 100] và `steering`.
4. Tên cột toạ độ và hướng: Nguồn A đặt tên là `lat`, `lon`, `alt_m`, `heading_deg`; Nguồn B đặt tên là `latitude`, `longitude`, `altitude`, `hdg`.
5. Đồng hồ thiết bị: Nguồn B bị lệch đồng hồ 1500 ms so với mốc video (khai báo trong `sources.json`).

Pipeline giữ mỗi nguồn một bảng bronze riêng biệt và lưu toàn bộ cột ở dạng chữ (raw text strings) nhằm tuân thủ nguyên lý Medallion Lakehouse: tầng Bronze đóng vai trò là "single source of truth" bất biến, ghi nhận trung thực nguyên bản dữ liệu từ cảm biến. Việc trì hoãn ép kiểu sang tầng Silver giúp ngăn chặn việc mất mát dữ liệu không thể phục hồi nếu xảy ra lỗi parse dữ liệu bẩn, đồng thời xử lý triệt để hiện tượng schema drift giữa các nhà cung cấp phần cứng khác nhau mà không làm gián đoạn luồng ingestion.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline sử dụng các model dbt staging để chuẩn hoá hai nguồn về schema chung:
- Đơn vị đo: Vận tốc nguồn B được đổi từ km/h sang m/s (`spd_kmh / 3.6`), phần trăm bàn đạp đổi về dải [0, 1].
- Kiểu thời gian: Cả hai nguồn đều được chuyển đổi về kiểu timestamp UTC tiêu chuẩn (`ts_utc`).
- Độ lệch đồng hồ: Nguồn B được trừ đi 1500 ms (`clock_offset_ms` khai báo trong `sources.json`) để đồng bộ trục thời gian với video.
Bằng chứng chuẩn hoá đúng: Thời điểm bắt đầu của telemetry B sau khi trừ offset là `2025-10-08 14:30:00.009000+00:00`, hoàn toàn trùng khớp với thời điểm bắt đầu video B (`14:30:00`). Vận tốc tối đa sau chuẩn hoá đạt 71.0 km/h (Drive A) và 100.0 km/h (Drive B), không còn giá trị vô lý.

Bảng số dòng bị cách ly (quarantine) từ `run_summary.json`:
| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Tổng cộng 106 dòng bị cách ly (53 dòng mỗi drive). Dòng dữ liệu sạch: Drive A còn 2968 dòng (2968 + 53 = 3021), Drive B còn 3181 dòng (3181 + 53 = 3234). Dữ liệu lỗi được chuyển vào `silver_telemetry_rejects` thay vì bị xoá âm thầm để đảm bảo tính minh bạch và khả năng kiểm toán (auditability). Kỹ sư hệ thống có thể theo dõi tần suất lỗi phần cứng/cảm biến để thực hiện bảo trì, và có thể tái xử lý dữ liệu khi cập nhật lại các ngưỡng làm sạch.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` được cấu hình là `warn` vì hiện tượng mất gói tin hoặc mất tín hiệu cảm biến trong khoảng thời gian ngắn (ví dụ xe đi qua hầm) là hiện tượng vật lý hoàn toàn có thể xảy ra trong thực tế; đây là thông tin cần cảnh báo người vận hành nhưng không được dừng toàn bộ pipeline. Ngược lại, test `in_range` (vận tốc [0, 70] m/s) là tiêu chí sống còn: nếu vận tốc âm hoặc vượt quá 252 km/h thì chắc chắn là lỗi cảm biến/nhiễu dữ liệu cực kỳ nghiêm trọng, bắt buộc phải dừng pipeline ở mức `error` để ngăn chặn dữ liệu độc hại lọt vào mô hình xe tự lái.

Kết quả thí nghiệm `break_gate.json`: Khi nới lỏng ngưỡng kiểm tra vận tốc từ 70 m/s lên 1000 m/s trong model dbt, test `in_range_silver_telemetry_speed_mps__70__0` lập tức bắt được chính xác **6 dòng** vi phạm (kết quả `PASS=5 WARN=1 ERROR=1 SKIP=0`). Khi khôi phục luật gốc, dbt test trở lại trạng thái `PASS=6 WARN=1 ERROR=0 SKIP=0`. Thí nghiệm chứng minh "Cổng chất lượng" (Quality Gate) đóng vai trò lá chắn sống còn, bảo đảm nguyên tắc "Fail fast": phát hiện lỗi ngay tại nguồn và ngăn dữ liệu rác lan truyền sang các tầng phân tích downstream.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa trên file kết quả `idempotency.json`:
- Thí nghiệm thực hiện trên partition `trip-00003` (chứa 60 khung hình).
- Số dòng trước khi chạy lại: 1246 dòng, dấu vân tay bảng (fingerprint) là `6c7480dfde7e`.
- Sau 2 lần chạy lại liên tiếp theo cơ chế pipeline: bảng vẫn có đúng 1246 dòng, fingerprint giữ nguyên `6c7480dfde7e` (hoàn toàn không thay đổi).
- Ngược lại, với phương thức ghi `append` sai lầm, sau 2 lần chạy số dòng của trip bị nhân đôi từ 60 lên **120 dòng** (2x duplication).

Cơ chế Delta Lake `replaceWhere` (overwrite theo partition `trip_id`) an toàn hơn việc xoá thủ công rồi ghi lại vì tính chất giao dịch nguyên tử (ACID transaction). Toàn bộ quá trình ghi file mới và cập nhật log diễn ra cô lập. Nếu tác vụ bị dừng đột ngột (crash/hết bộ nhớ), transaction sẽ tự động rollback, dữ liệu cũ vẫn nguyên vẹn. Ngược lại, thao tác "xoá trước ghi sau" không bảo đảm tính nguyên tử: nếu gặp sự cố giữa chừng, bảng dữ liệu sẽ bị trống hoặc mất vĩnh viễn phân vùng đó.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình **đen** (`is_black`) có độ sáng trung bình cực thấp (luma < 12.0), sinh ra do nắp đậy camera chưa mở, mất tín hiệu cảm biến quang học hoặc lỗi phần cứng; khung này hoàn toàn không có thông tin thị giác nên pipeline loại bỏ hoàn toàn. Khung hình **tối** (`is_dark`) phản ánh điều kiện thiếu sáng tự nhiên ngoài đời thực (đi vào hầm, bóng cây râm, ban đêm); khung này vẫn chứa các đối tượng vật thể và đường đi, do đó pipeline gắn cờ cảnh báo nhưng **giữ lại** (`dark_kept`: Drive A có 4 khung, Drive B có 61 khung). Tôi hoàn toàn đồng ý với thiết kế này vì mô hình thị giác xe tự lái bắt buộc phải học cách nhận diện vật thể trong điều kiện thiếu sáng để vận hành ban đêm an toàn.

Bảng độ nhạy ngưỡng trùng lặp từ `threshold.json`:
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi lựa chọn **ngưỡng 2 bit** (ngưỡng chuẩn của pipeline). Nếu chọn ngưỡng 0, các khung hình chỉ chênh lệch vài pixel do nhiễu cảm biến sẽ bị bỏ sót. Nếu chọn ngưỡng 6, số lượng khung bị loại quá lớn (363 khung ở Drive A và 244 ở Drive B), có nguy cơ loại bỏ nhầm cả các khung hình có biến chuyển ngoại cảnh thực tế. Ngưỡng 2 loại bỏ chính xác các trường hợp camera bị freeze (đứng hình 6 giây ở Drive A) và khi xe đứng yên. Ngưỡng này phụ thuộc chặt chẽ vào vận tốc: Khi xe chạy nhanh, ngoại cảnh thay đổi liên tục khiến khoảng cách dHash giữa các frame liên tiếp lớn (ít trùng); khi xe dừng hoặc chạy rất chậm, các frame liên tiếp gần như tương đồng khiến khoảng cách dHash nhỏ hơn ngưỡng và bị đánh dấu trùng.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Số liệu từ file `leakage.json` (tổng số 1019 khung hình thuộc 26 trip):
- Khi chia ngẫu nhiên **theo khung hình** (tỷ lệ 70/15/15): Có tới **26/26 trip (100%)** bị chia cắt giữa các tập split, và toàn bộ 26 trip đều có khung hình xuất hiện đồng thời ở cả tập Train lẫn tập Test.
- Khi chia **theo trip** (cơ chế của pipeline): **0 trip** bị chia cắt (trips straddling = 0).

Việc chia theo khung hình tạo ra hiện tượng rò rỉ dữ liệu (Data Leakage) nghiêm trọng. Do tần số lấy mẫu là 2 fps, hai khung hình liên tiếp cách nhau chỉ 0.5 giây trong cùng một chuyến đi gần như giống hệt nhau về góc máy, con đường, thời tiết và cảnh nền. Khi chia theo frame, mô hình đã "học thuộc" cảnh vật tại frame t trong tập train và gặp lại đúng cảnh vật đó ở frame t+0.5s trong tập test. Kết quả là điểm kiểm thử (Accuracy/Loss) cao một cách giả tạo vì mô hình chỉ ghi nhớ (memorize) cảnh nền quen thuộc, thay vì học được khả năng khái quát hoá (generalize). Khi triển khai xe chạy trên một cung đường hoàn toàn mới, mô hình sẽ thất bại.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` (`cd79f04deba6`) được tính bằng mã băm nội dung SHA-256 từ `manifest.json` (chứa danh sách chính xác các frame_id, nhãn split, hash video nguồn và tham số phân chia). Sử dụng hàm băm nội dung thay vì ngày giờ hay số đếm tuần tự giúp phiên bản có tính chất **xác định (deterministic)** và **bất biến (idempotent)**: Hai lần chạy trên cùng một nội dung dữ liệu sẽ luôn sinh ra cùng một mã version duy nhất. Điều này cho phép hệ thống dễ dàng kiểm toán, tránh duplicate dataset trong kho lưu trữ và đảm bảo khả năng tái lập thí nghiệm 100%.

Hai lần chạy liên tiếp với cùng dữ liệu và cấu hình cho ra `dataset_version` hoàn toàn giống nhau (`cd79f04deba6`). Phiên bản của tôi khác biệt so với bạn bè là do pipeline sử dụng biến môi trường `LAB_STUDENT_ID` (`2A202602372`) làm giá trị salt trong model dbt: `md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`. MSSV khác nhau sinh ra mã băm phân bổ trip khác nhau, dẫn đến danh sách các frame gán vào từng tập train/val/test khác nhau. Vì nội dung manifest khác nhau, mã băm `dataset_version` được tạo ra là duy nhất cho mỗi học viên.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Nội dung phần `lineage` được trích xuất từ file `manifest.json`:
- Hai video gốc với mã băm SHA-256:
  - `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  - `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- Phiên bản commit của các bảng Delta Lake upstream:
  - `bronze.raw_telemetry_a`: version 0
  - `bronze.raw_telemetry_b`: version 0
  - `bronze.trips_raw`: version 0
  - `bronze.video_files`: version 1
  - `silver.frames`: version 25
  - `silver.telemetry`: version 0
  - `silver.frames_flagged`: version 0
  - `silver.frame_telemetry`: version 0

Nếu 3 tháng sau một người cần tái tạo lại chính xác dataset này, ba thông tin cốt lõi bắt buộc phải có là:
1. Hai mã băm SHA-256 của video gốc để xác minh tính toàn vẹn của dữ liệu thô.
2. Số phiên bản commit của các bảng Delta Lake (nhờ tính năng Time Travel của Delta Lake, ta có thể truy vấn lại chính xác trạng thái dữ liệu tại commit tương ứng).
3. Giá trị MSSV `LAB_STUDENT_ID` (`2A202602372`) cùng tỷ lệ phân chia split (70/15/15) được dùng làm salt phân bổ trip.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Khi thiết lập `LAB_FAIL_ONCE=trip-02689` trong `.env` và kích hoạt pipeline trong Airflow:
- Đúng **1 task** `silver_frames` (tại map_index 6 tương ứng với trip `trip-02689`) chuyển sang màu đỏ (**failed**) với thông báo lỗi giả lập: `simulated crash while extracting trip-02689`.
- Toàn bộ **6 task** downstream (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`) chuyển sang màu cam (**upstream_failed**).
- Thao tác xử lý: Chỉ cần nhấp vào task `silver_frames` bị lỗi và chọn **Clear** (hoặc dùng lệnh `airflow tasks clear`).
- Kết quả chạy lại: Task `silver_frames` chạy thành công, kích hoạt các task downstream tiếp tục thực thi và toàn bộ DAG chuyển màu xanh (**success**).
- Dữ liệu hoàn toàn **không bị nhân đôi**: Nhờ cơ chế Delta partition overwrite theo `trip_id`, chỉ riêng partition `trip-02689` được cập nhật lại nguyên tử. Số lượng khung hình gold vẫn là 1019, và `dataset_version` giữ nguyên là `cd79f04deba6`.
- Bằng chứng: Ảnh chụp `report/screenshots/airflow_task_crash.png` thể hiện rõ task failed tại map_index 6 và các task downstream bị upstream_failed, cùng kết quả verify đạt 27/27 kiểm tra.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi quy mô mở rộng lên 500 xe/ngày (hàng chục TB video và hàng trăm triệu dòng telemetry mỗi ngày), ba thành phần sẽ vỡ đầu tiên:
1. **Quá trình giải mã video FFmpeg đơn máy (Single-node decoding)**: Hiện tại việc giải mã video chạy trên 1 container Airflow duy nhất. Khi có hàng nghìn giờ video, CPU/RAM sẽ quá tải nghiêm trọng dẫn đến nghẽn cổ chai và OOM crash.
   *Giải pháp thay thế (slide 23, 24)*: Sử dụng cụm tính toán phân tán **Apache Spark on Kubernetes / Ray Cluster** để phân tán việc decode song song các video chunks trên hàng trăm worker nodes có gắn GPU/CPU tăng tốc.
2. **DuckDB xử lý ELT cục bộ (Single-node Embedded Engine)**: DuckDB chỉ chạy trên 1 file database cục bộ `lakehouse.duckdb`. Nó sẽ bị giới hạn bởi RAM/Disk của máy chủ và không hỗ trợ ghi đồng thời (concurrent write) từ hàng nghìn worker.
   *Giải pháp thay thế (slide 16, 22)*: Thay thế bằng data lakehouse phân tán thực thụ như **Databricks Delta Lake, Google Cloud BigQuery hoặc Spark SQL**, kết hợp với dbt adapter tương ứng.
3. **MinIO đơn node (Single-node Object Storage)**: MinIO triển khai trên 1 container Docker volume sẽ nhanh chóng cạn kiệt dung lượng ổ đĩa và chịu nút thắt nghẽn băng thông I/O (I/O throughput bottleneck).
   *Giải pháp thay thế (slide 17)*: Chuyển sang dịch vụ Object Storage đám mây chuẩn doanh nghiệp với khả năng mở rộng vô hạn như **AWS S3, Google Cloud Storage (GCS) hoặc Azure Blob Storage**.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
    t.lighting,
    t.weather,
    COUNT(DISTINCT t.trip_id) AS total_trips,
    SUM(t.total_frames) AS total_frames,
    SUM(t.black_frames) AS black_frames,
    SUM(t.blurry_frames) AS blurry_frames,
    SUM(t.dark_frames) AS dark_frames,
    ROUND(AVG(t.clean_ratio) * 100, 2) AS avg_clean_ratio_pct
FROM marts.mart_trip_summary t
GROUP BY t.lighting, t.weather
ORDER BY avg_clean_ratio_pct ASC;
```

**Ý nghĩa và giá trị phân tích**:
Câu truy vấn này phân tích chất lượng dữ liệu khung hình thu thập được dựa trên bối cảnh môi trường thực tế (điều kiện ánh sáng `lighting` và thời tiết `weather`). Kết quả cho thấy tỷ lệ khung hình sạch (`avg_clean_ratio_pct`) giảm rõ rệt trong các điều kiện ánh sáng yếu (đường hầm, chập tối) do số lượng `dark_frames` tăng cao. Thông tin này giúp đội ngũ Data Science hiểu rõ sự mất cân bằng dữ liệu (data imbalance) theo từng điều kiện môi trường để lên kế hoạch thu thập bổ sung dữ liệu có chủ đích (targeted data collection), nâng cao độ bền vững của mô hình xe tự lái khi hoạt động trong điều kiện thời tiết xấu và ban đêm.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- Điều bất ngờ nhất: Tầm quan trọng của việc chia split theo cảnh/trip thay vì ngẫu nhiên theo frame. Thí nghiệm 3 chứng minh chia theo frame gây rò rỉ 100% chuyến đi, giải thích vì sao nhiều mô hình deep learning có kết quả benchmark rất cao trong phòng thí nghiệm nhưng lại thất bại khi triển khai thực tế.
- Điều chưa rõ: Chiến lược nén và quản lý phiên bản tối ưu của Delta Lake khi lưu trữ lâu dài hàng triệu con trỏ ảnh URI và các vector embedding thị giác lớn.
- Lỗi dữ liệu pipeline chưa bắt được: Pipeline mới chỉ lọc khung đen, mờ toàn cục (Laplacian blur) và trùng lặp dHash, nhưng chưa phát hiện được lỗi camera bị bẩn một phần (nước mưa, bùn đất bám cục bộ trên ống kính) hoặc hiện tượng chói lóa ánh sáng cực đại (glare/lens flare do đèn pha xe đối diện chiếu thẳng vào ban đêm).
