# ADR-004: Use SCD Type 1 for Customer Attributes

Status: Accepted for the batch MVP.

Customer dimension dùng `customer_unique_id` làm business key và MD5 hex làm stable
surrogate key. Chọn current attributes từ customer record có `source_sequence` lớn
nhất, tie-break theo customer_id DESC; giữ order-scoped `customer_id` trong staging/fact để trace.

Type 1 phù hợp phạm vi MVP và không cần tạo lịch sử giả từ snapshot Olist tĩnh.
Khi location thay đổi, phân tích các order cũ sẽ dùng location hiện tại; không coi
đây là địa chỉ tại thời điểm mua. SCD Type 2 và business-effective intervals chỉ
được thêm khi có dữ liệu lịch sử phù hợp và tests cho temporal joins.

Quy tắc model, hash, timestamps và validation được mô tả trong
[Analytics Models](../data_contracts/09_analytics_models.md).

Dimension keys là deterministic identifiers, không phải security hashes. Missing
required parents đi theo Silver deferred/reject và dbt integrity gates; không tạo
synthetic unknown members để thay FK. Optional location giữ NULL khi thiếu hoặc
không match ZIP. Xem [dimension key policy](../data_contracts/09_analytics_models.md#dimension-keys-and-missing-members)
cho mapping của cả năm dimensions, empty/retained staging và reviews-only backfill.
