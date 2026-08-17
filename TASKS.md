# TASKS — LAB 17 Data Pipeline Engineering

Trạng thái sau khi cài môi trường: **1/4 tiêu chí đạt** (chỉ có `gold_doc_chunks` —
nhóm đối chứng, không có lỗi).

## Môi trường (đã xong)

- `.venv` Python 3.11 (không dùng 3.14 — dbt-core chưa hỗ trợ)
- Junction `.venv\bin` → `.venv\Scripts` để Makefile kiểu POSIX chạy trên Windows
- duckdb 1.5.5 · dbt-core 1.12.2 · dbt-duckdb 1.11.0
- Seed 14 ngày + `data/gold_events/` (5.000 parquet cho bài mở rộng)

Mỗi lần mở terminal Git Bash mới, chạy trước:

```bash
export PYTHONUTF8=1 PYTHONIOENCODING=utf-8
```

Không có biến này thì `seed/generate.py` crash vì console Windows dùng cp1252.

Hàm query (terminal thứ hai):

```bash
q() { PYTHONUTF8=1 .venv/bin/python -c "
import duckdb, sys
duckdb.connect('warehouse.duckdb').sql(sys.argv[1]).show(max_rows=40)
" "$1"; }
```

---

## Nhiệm vụ 1 — `gold_training_set` tăng row mỗi lần chạy

Hiện tại: 38.750 row, kỳ vọng 12.480. Checksum lệch qua 3 lượt → không idempotent.

- [ ] `dbt/models/gold/gold_training_set.sql` — bổ sung `unique_key` +
      `incremental_strategy` vào `config()`. Grain là **entity** (1 ticket), không
      phải event; source có `op = 'u'` nên một ticket rơi vào hai partition ngày
      khác nhau trong cùng một lượt.
- [ ] `dags/ai_training_pipeline.py` — set `catchup` và `max_active_runs` ở phần
      `TODO`. Hiện là `True / None`.

Xong khi: `gold_training_set` = 12.480, `ỔN ĐỊNH` ✓, dòng `1 hàng / 1 ticket` ✓,
dòng `DAG: catchup / max_active_runs` ✓.

Lưu ý: hai parameter DAG chỉ giảm tần suất kích hoạt, **không phải root cause**.

---

## Nhiệm vụ 2 — `gold_feature_daily` thiếu row ở ngày cũ

Hiện tại: 8.645 row, kỳ vọng 9.100 (14 ngày × 650 customer). Thiếu 455.

- [ ] Đo P99 độ trễ `event_time` → `_ingested_at` trên `bronze_events`.
      **Ghi con số này vào report** — rubric bắt buộc.
- [ ] `dbt/models/gold/gold_feature_daily.sql` — thay
      `where event_date > (select max(event_date) from {{ this }})` bằng lookback
      window đủ rộng, căn cứ P99.
- [ ] Cùng file — thêm `unique_key` dạng list hai cột `(event_date, customer_id)`,
      nếu không window rộng hơn sẽ cộng dồn đúng như lỗi nhiệm vụ 1.

Xong khi: `gold_feature_daily` = 9.100 và `ỔN ĐỊNH` ✓, `gold_training_set` giữ 12.480.

---

## Nhiệm vụ 3 — Kiểu dữ liệu cột `priority` đổi giữa chu kỳ

Hiện tại: 6.606 row `priority` sai/NULL, `quarantine_tickets` = 0 (kỳ vọng 312).

Ba nhóm giá trị `priority_raw`, xử lý khác nhau:

| Nhóm | Ví dụ | Xử lý |
|---|---|---|
| Số hợp lệ | `1` `2` `3` `4` | giữ nguyên |
| Nhãn chuỗi | `urgent` `high` `medium` `low` | map → `1` `2` `3` `4` |
| Không hợp lệ | `P1` `unknown` `0` `5` `-1` `''` `null` | quarantine |

- [ ] `dbt/macros/normalize_priority.sql` — thay `try_cast(...)` bằng `CASE` đủ ba
      nhóm, trả `NULL` cho nhóm 3. Cả hai model dùng chung macro này.
- [ ] `dbt/models/silver/silver_tickets.sql` — **lọc trước, `row_number` sau**.
      Làm ngược lại thì số ticket tụt 12.480 → 12.168.
- [ ] `dbt/models/silver/quarantine_tickets.sql` — thay `where false` bằng điều
      kiện "macro trả về NULL".
- [ ] `dbt/models/silver/schema.yml` — `enforced: true`, bỏ comment khối `tests:`
      ở cột `priority`, điền danh sách giá trị hợp lệ.

Xong khi: `dbt test` ✓ với **> 9 test**, `priority ∈ 1..4 không NULL` ✓,
`quarantine_tickets` = 312 và `ỔN ĐỊNH` ✓, `gold_training_set` giữ 12.480.

---

## Report

Dùng `REPORT_TEMPLATE.md`. Mỗi nhiệm vụ bốn mục: Triệu chứng / Root cause /
Cách fix / Bằng chứng.

**Root cause chiếm 10 điểm cuối.** "Thêm `unique_key`" là *cách fix*. Root cause
phải là phát biểu về cơ chế.

Hai con số bắt buộc: P99 độ trễ (nhiệm vụ 2), và số row trước/sau của từng bảng.

---

## Mở rộng (không bắt buộc) — `EXTRA.md`

- [ ] Nhiệm vụ 4 — `make compact` gộp 5.000 file parquet nhỏ. Hiện `1.0×`, cần `≥ 10×`.

## Lệnh hay dùng

```bash
make quick      # 1 lượt, kiểm tra nhanh
make verify     # 3 lượt + bảng chấm (~95s)
make clean && make pipeline    # khi verify lỗi lạ sau nhiều lần sửa
cat dbt/target/run/lab17/models/gold/gold_training_set.sql   # xem SQL dbt sinh thật
```
