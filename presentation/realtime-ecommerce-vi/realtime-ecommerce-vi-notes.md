# Real-time E-commerce Insights — ghi chú thuyết trình

> Tài liệu này không chiếu. Mỗi mục tương ứng một slide trong `index.html`.

## Slide 1 — Mở đầu

Dự án mô phỏng nền tảng thương mại điện tử phát sự kiện đơn hàng liên tục. Hệ thống xử lý created, updated và cancelled, tái dựng trạng thái hiện tại, sau đó tạo các bảng doanh thu, vận tốc bán và dự báo cho Power BI. Stack local dùng PySpark, Redpanda/Kafka API và Docker.

## Slide 2 — Bài toán

Báo cáo batch cuối ngày không đủ nhanh cho vận hành bán hàng. Một đơn hàng còn thay đổi sau khi tạo, nên cộng trực tiếp mọi event sẽ làm doanh thu bị nhân đôi hoặc vẫn tính đơn đã hủy. Pipeline cần giữ lịch sử sự kiện nhưng đồng thời xác định đúng trạng thái hiện tại.

## Slide 3 — Event contract

`event_id` là khóa chống xử lý trùng. `order_id` gom các thay đổi của cùng một đơn. `event_type` mô tả chuyển đổi trạng thái. `event_ts` phải dùng UTC để sắp xếp nhất quán. Province và amount phục vụ phân tích vùng và doanh thu. Nếu contract sai, bản ghi đi vào quarantine với lý do cụ thể.

## Slide 4 — Kiến trúc tổng thể

Producer phát event vào Redpanda qua Kafka API. Repo cũng có JSONL source để CI chạy xác định. Spark Structured Streaming đọc event, ghi Bronze, tạo Silver hợp lệ và quarantine, sau đó tái dựng current order state. Gold tạo Daily Sales, Province Revenue, Sales Velocity và Forecast 7 ngày. Power BI chỉ đọc các data product này.

## Slide 5 — Vì sao có Kafka và JSONL

Kafka path kiểm chứng tích hợp streaming thật: topic, offset, producer/consumer và checkpoint. File path kiểm chứng transform một cách nhanh, ổn định, không phụ thuộc timing của broker trong CI. Hai đường cùng dùng event contract và logic nghiệp vụ. Không nên trình bày JSONL là streaming production; nó là test harness xác định.

## Slide 6 — Structured Streaming và checkpoint

Mỗi micro-batch đọc phần offset mới, áp dụng transform, ghi kết quả rồi cập nhật checkpoint. Khi chạy lại mà không có event mới, hệ thống phải xử lý 0 bản ghi mới. Đây là bằng chứng quan trọng của khả năng replay an toàn. Smoke test Kafka của repo đã quan sát high-watermark 51.

## Slide 7 — Medallion

Bronze trả lời “đã nhận gì” và giữ occurrence. Silver trả lời “event nào hợp lệ” sau dedup và DQ. Current order state trả lời “mỗi order đang ở trạng thái nào” bằng cách chọn event mới nhất. Quarantine giữ bản ghi lỗi để điều tra, thay vì âm thầm bỏ.

## Slide 8 — Current state và cancellation

Event history là append-only; một order có thể có nhiều dòng. Dùng window theo `order_id`, sắp xếp `event_ts` giảm dần và chọn dòng đầu để lấy trạng thái mới nhất. Nếu trạng thái là cancelled, đơn vẫn còn trong lịch sử và báo cáo chất lượng, nhưng không được tính vào doanh thu active.

## Slide 9 — Gold và forecast

Daily Sales phục vụ theo dõi ngày. Province Revenue tạo nền cho heatmap Việt Nam. Sales Velocity đo tốc độ bán theo cửa sổ thời gian. Forecast 7 ngày cung cấp tín hiệu ngắn hạn. Forecast trong portfolio là baseline có thể giải thích; khi production cần bổ sung đánh giá drift, mùa vụ và external features.

## Slide 10 — Power BI

Sales Pulse trả lời hôm nay bán thế nào. Vietnam Map trả lời doanh thu đến từ đâu. 7-day Outlook trả lời xu hướng sắp tới. Logic current state và loại cancelled nằm trước Power BI để tất cả dashboard dùng cùng một định nghĩa doanh thu.

## Slide 11 — Chạy local

Docker Compose khởi động Redpanda và các thành phần cần thiết. `make demo` chạy đường dữ liệu mẫu; `make kafka-smoke` kiểm chứng producer/broker; `make test` chạy test. Không cần tài khoản Azure. Đây là môi trường tái hiện kiến trúc và hành vi cốt lõi trên laptop.

## Slide 12 — Kết quả demo

Bronze có 1.428 event. Sau deduplicate còn 1.400, nghĩa là phát hiện 28 duplicate. Trong số 1.400 event duy nhất, 1.358 accepted và 42 quarantined, khớp phương trình đối soát. Current state có 1.183 order, gồm 1.150 active và 33 cancelled. Doanh thu active trong demo là 8.750.790.000 VND.

## Slide 13 — Local sang Azure

Các pattern có thể ánh xạ lên Azure: Kafka/Redpanda sang Event Hubs hoặc Kafka managed; filesystem/MinIO sang ADLS/Delta; PySpark sang Databricks; orchestration sang dịch vụ managed; Power BI Desktop sang Power BI Service. Tuy nhiên repo không tuyên bố đã triển khai Azure thật — đây là cách mô tả trung thực và thuyết phục.

## Slide 14 — Giá trị portfolio

Dự án thể hiện event modeling, schema enforcement, checkpoint, replay, idempotency, window function, state reconstruction, data quality và data mart. Điểm phỏng vấn mạnh nhất là giải thích vì sao tách deterministic transform tests khỏi Kafka integration test.

## Slide 15 — Kết

Thông điệp một câu: streaming có ích khi hệ thống đưa ra đúng trạng thái và đúng số liệu đủ sớm để hành động.
