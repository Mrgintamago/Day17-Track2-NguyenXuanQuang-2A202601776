# Báo cáo LAB 17: Data Pipeline Engineering

**Họ tên:** Nguyen Xuan Quang  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 88.1s
  run 2/3 … 63.7s
  run 3/3 … 64.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau mỗi lần chạy lại pipeline cho cùng một ngày, `gold_training_set` tăng thêm hàng thay vì giữ nguyên. Sau 3 lượt: 38.750 hàng, trong khi kỳ vọng chỉ 12.480 (1 hàng/ticket). Không có báo lỗi nào, pipeline "chạy được" nhưng dữ liệu sai. |
| **Nguyên nhân** | `gold_training_set` là model `incremental` nhưng chưa khai `unique_key`, nên dbt không có cách nào biết "hàng nào là cùng một hàng": nó sinh câu lệnh `INSERT` thuần, ghi thêm thay vì ghi đè. Đồng thời source CDC có bản ghi `op='u'`: một ticket tạo ngày D1 rồi sửa ngày D2 sẽ đi qua điều kiện lọc `_ingested_at` của **cả D1 lẫn D2** trong cùng một lượt chạy toàn bộ 14 ngày, tạo ra nhiều hơn một dòng insert cho cùng một `ticket_id`. Vì grain của bảng là **entity** (1 ticket) chứ không phải event, chiến lược `insert`/`delete+insert` theo partition ngày không đủ, cần `merge` theo khoá tự nhiên (`ticket_id`) để lần ghi sau thay thế đúng dòng cũ bất kể nó rơi vào partition ngày nào. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`. `dags/ai_training_pipeline.py`: đặt `catchup=False`, `max_active_runs=1`. Hai tham số này không sửa root cause nhưng giảm tần suất kích hoạt tình huống (bấm Clear Task trong lúc `catchup=True`/không giới hạn `max_active_runs` khiến nhiều run ghi chồng lên nhau cùng lúc). |
| **Bằng chứng** | trước: 38.750 hàng (kỳ vọng 12.480) · sau: 12.480 hàng · checksum 3 lượt: `8dd7c98653` cả ba lần (giống hệt nhau) · dòng `1 hàng / 1 ticket`: ✓ |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 hàng trong khi kỳ vọng 9.100 (14 ngày × 650 customer), thiếu 455 hàng. Phần thiếu tập trung ở những ngày đã chạy xong từ lâu, còn ngày mới nhất thì đủ. |
| **P99 độ trễ đo được** | **2,73 ngày** *(bắt buộc)*, đo trên `date_diff('second', event_time, _ingested_at)` của `bronze_events`. Thống kê đầy đủ: P50 = 0,13 ngày, P95 = 1,81 ngày, P99 = 2,73 ngày, max = 2,94 ngày; ~5,05% tổng số bản ghi tới kho muộn hơn 1 ngày so với lúc sự kiện xảy ra. |
| **Lookback đã chọn** | **3 ngày**, vì P99 = 2,73 ngày và max quan sát được = 2,94 ngày; lùi 3 ngày tròn phủ hết toàn bộ đuôi phân phối đo được, kể cả max, mà không phải quét lại toàn bộ lịch sử. |
| **Nguyên nhân** | Điều kiện lọc cũ `where event_date > (select max(event_date) from {{ this }})` so sánh theo `event_date`: mốc **ngày sự kiện xảy ra**, không phải mốc **ngày dữ liệu tới kho**. Một bản ghi có `event_date = 08-12` nhưng `_ingested_at = 08-15` (tới muộn 3 ngày): tại lượt chạy 08-15, `max(event_date)` trong bảng đích lúc đó thường đã là 08-14 hoặc mới hơn (các ngày sau đã "chốt" ở lượt chạy trước), nên `event_date = 08-12` không còn thoả `>`. Bản ghi này vĩnh viễn không được tính, kể cả ở các lượt chạy sau, vì nó thuộc về một `event_date` đã "khép lại". Đây là hệ quả trực tiếp của việc dùng mốc business time (event_date) làm watermark thay vì có lookback bù cho ingestion time. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện thành `where event_date > (select max(event_date) from {{ this }}) - interval 3 day` để các ngày gần watermark được tính lại thay vì bị đóng băng vĩnh viễn. Vì cùng một cặp `(event_date, customer_id)` giờ có thể được tính lại ở nhiều lượt chạy, thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để lần tính sau **thay thế** lần tính trước thay vì cộng dồn (tránh lặp lại đúng lỗi của nhiệm vụ 1 trên một bảng khác). |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng (đúng 14×650) · ổn định 3 lượt, checksum `3db448685c` cả ba lần · `gold_training_set` không đổi, vẫn 12.480 |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Dùng `max` (2,94 ngày) sẽ an toàn tuyệt đối cho tập dữ liệu quan sát được, nhưng `max` là một giá trị cực trị: chỉ cần một bản ghi lỗi đồng hồ hoặc một sự cố mạng hy hữu làm trễ dữ liệu vài chục ngày cũng đủ kéo `max` lên rất cao, buộc lookback window phải nới theo, và **mỗi ngày nới thêm là chi phí quét lại dữ liệu tốn kém ở MỌI lượt chạy sau này**, không phải chi phí một lần. P99 phản ánh hành vi "bình thường" của 99% dữ liệu, ổn định hơn nhiều trước outlier, nên được dùng làm căn cứ chọn window; phần đuôi ngoài P99 (ở đây trùng khớp gần với max nên window 3 ngày phủ được cả hai) được chấp nhận là rủi ro còn lại, đánh đổi lấy chi phí vận hành thấp hơn.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | 6.606 hàng trong `silver_tickets` có `priority` sai hoặc NULL, trong khi `quarantine_tickets` (nơi lẽ ra phải chứa các bản ghi lỗi này) lại rỗng (kỳ vọng 312 hàng). Pipeline không dừng, `dbt test` vẫn pass 9/9, nhưng chất lượng dữ liệu đầu ra cho model phân loại giảm hẳn kể từ một mốc thời gian nhất định. |
| **Nguyên nhân** | Từ 08-10, team backend đổi cách biểu diễn `priority_raw` từ số (`'1'..'4'`) sang nhãn chữ (`'urgent'`, `'high'`, `'medium'`, `'low'`), một thay đổi **schema evolution hợp lệ về ý nghĩa**, chỉ khác cách biểu diễn. Biểu thức chuẩn hoá cũ `try_cast(priority_raw as integer)` sai theo **hai hướng cùng lúc**: (1) nó không parse được nhãn chữ nên trả `NULL` cho toàn bộ nhóm dữ liệu hợp lệ này (coi nhầm dữ liệu tốt là lỗi); (2) nó vẫn chấp nhận các chuỗi số nằm ngoài miền hợp lệ như `'0'`, `'5'`, `'-1'` vì chúng parse thành integer được, dù contract quy định `priority ∈ 1..4` (bỏ lọt dữ liệu thật sự sai). Vì `quarantine_tickets.sql` còn nguyên `where false`, không có cơ chế nào tách các bản ghi hỏng thật ra khỏi luồng, nên chúng đi thẳng vào `silver_tickets` dưới dạng NULL. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) Số hợp lệ `1`-`4`: giữ nguyên. (2) Nhãn chuỗi `urgent/high/medium/low`: map về `1/2/3/4` theo tài liệu API của backend, đây là schema evolution, không phải lỗi. (3) Giá trị không hợp lệ (`P1`, `unknown`, `0`, `5`, `-1`, rỗng, NULL): đưa vào quarantine, không đoán giá trị thay thế. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng một khối `CASE` xử lý đủ ba nhóm, trả `NULL` cho nhóm 3. Macro dùng chung cho cả `silver_tickets` và `quarantine_tickets` nên hai bảng không thể lệch nhau. `dbt/models/silver/silver_tickets.sql`: lọc bỏ bản ghi có `priority` chuẩn hoá ra `NULL` **trước** khi xếp hạng `row_number`, để chỉ loại đúng bản ghi CDC hỏng. Nếu ticket đó còn một lần cập nhật hợp lệ trước đó, ticket vẫn giữ trong Silver với trạng thái cũ, không bị loại cả ticket. `dbt/models/silver/quarantine_tickets.sql`: `where` bằng điều kiện macro trả `NULL`. `dbt/models/silver/schema.yml`: bật `contract.enforced: true` (ràng buộc kiểu dữ liệu) và thêm test `not_null` + `accepted_values: [1,2,3,4]` cho cột `priority` (ràng buộc miền giá trị, hai việc khác nhau: contract một mình vẫn cho `priority = 99` lọt qua). |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (đúng kỳ vọng) · `silver_tickets.priority` sạch, không còn NULL, luôn ∈ 1..4 · `silver_tickets` vẫn giữ đủ 12.480 ticket · `dbt test` ✓ 11/11 pass (tăng từ 9 lên 11 test) |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn ở **Silver**, không phải Bronze. Bronze là bản sao thô, trung thực của nguồn. Nếu Bronze từ chối luôn những bản ghi có `priority_raw` bất thường, dữ liệu gốc để điều tra sự cố (ví dụ đối chiếu lại đúng những gì backend đã gửi, xác định mốc thời gian đổi format) sẽ biến mất khỏi hệ thống, gây khó khăn cho việc truy vết về sau. Giữ nguyên ở Bronze rồi lọc/phân loại ở Silver cho phép vừa bảo toàn dữ liệu gốc, vừa tách được dữ liệu sạch khỏi dữ liệu lỗi.
>
> Không nên để `dbt test` fail và dừng cả DAG vì quy mô lỗi rất nhỏ so với tổng thể: 312 bản ghi lỗi so với 12.480 ticket hợp lệ và hơn 130.000 event, 31.200 chunk khác đang chờ phục vụ cho RAG index và routing agent. Một vài trăm bản ghi hỏng không có quyền chặn toàn bộ lượng dữ liệu tốt còn lại đến tay người dùng cuối; quarantine cho phép pipeline chạy tiếp bình thường trong khi bản ghi lỗi được xếp vào hàng đợi để người trực xử lý riêng.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | không làm |
| **Nguyên nhân** | — |
| **Cách khắc phục** | — |
| **Bằng chứng** | — |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Với mọi model `incremental`, kiểm tra `unique_key` và `incremental_strategy` có được khai báo đúng grain (entity hay event) không. Thiếu key là nguyên nhân phổ biến nhất khiến pipeline "chạy được" nhưng âm thầm nhân bản dữ liệu. |
| 2 | Đối chiếu mốc thời gian mà điều kiện `is_incremental()` dùng để lọc: đó là business time (`event_date`) hay ingestion time (`_ingested_at`)? Nếu là business time, phải đo phân phối độ trễ thực tế (P99) để biết cần lookback window bao xa. |
| 3 | Khi một cột nguồn có thể đổi kiểu/định dạng, kiểm tra xem logic chuẩn hoá hiện tại phân biệt được "dữ liệu đổi định dạng nhưng vẫn hợp lệ" với "dữ liệu thật sự lỗi" hay không. Gộp chung hai loại này là lỗi phổ biến nhất khi source đổi schema. |
