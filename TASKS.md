# TASKS — LAB 17 Data Pipeline Engineering

Trạng thái hiện tại: **4/4 tiêu chí đạt** — cả 3 nhiệm vụ chính đã xong, đã commit
và push (`ea556c8`, `09ea8a5`, `a2ce84d`). Còn lại: viết báo cáo theo
`REPORT_TEMPLATE.md`.

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

## Nhiệm vụ 1 — `gold_training_set` tăng row mỗi lần chạy — ✅ XONG

Đã sửa (commit `ea556c8`):

- [x] `dbt/models/gold/gold_training_set.sql` — `unique_key = 'ticket_id'`,
      `incremental_strategy = 'merge'`. Grain là **entity** (1 ticket); source
      có `op = 'u'` nên một ticket rơi vào hai partition ngày khác nhau trong
      cùng một lượt → cần merge theo khoá, không phải append.
- [x] `dags/ai_training_pipeline.py` — `catchup=False`, `max_active_runs=1`.

Kết quả: `gold_training_set` = 12.480, `ỔN ĐỊNH` ✓, `1 hàng / 1 ticket` ✓,
`DAG: catchup / max_active_runs` ✓.

Lưu ý: hai parameter DAG chỉ giảm tần suất kích hoạt, **không phải root cause**.

---

## Nhiệm vụ 2 — `gold_feature_daily` thiếu row ở ngày cũ — ✅ XONG

Đã sửa (commit `09ea8a5`):

- [x] Đo P99 độ trễ `event_time` → `_ingested_at` trên `bronze_events`:
      **P99 = 2,73 ngày** (max = 2,94 ngày; ~5% bản ghi tới muộn hơn 1 ngày).
- [x] `dbt/models/gold/gold_feature_daily.sql` — đổi điều kiện lọc thành
      `where event_date > (select max(event_date) from {{ this }}) - interval 3 day`
      (lùi 3 ngày để phủ hết P99).
- [x] Cùng file — thêm `unique_key = ['event_date', 'customer_id']` +
      `incremental_strategy = 'merge'` để window rộng hơn không cộng dồn.

Kết quả: `gold_feature_daily` = 9.100, `ỔN ĐỊNH` ✓, `gold_training_set` giữ 12.480.

---

## Nhiệm vụ 3 — Kiểu dữ liệu cột `priority` đổi giữa chu kỳ — ✅ XONG

Đã sửa (commit `a2ce84d`):

Ba nhóm giá trị `priority_raw`, xử lý khác nhau:

| Nhóm | Ví dụ | Xử lý |
|---|---|---|
| Số hợp lệ | `1` `2` `3` `4` | giữ nguyên |
| Nhãn chuỗi | `urgent` `high` `medium` `low` | map → `1` `2` `3` `4` |
| Không hợp lệ | `P1` `unknown` `0` `5` `-1` `''` `null` | quarantine |

- [x] `dbt/macros/normalize_priority.sql` — `CASE` xử lý đủ ba nhóm, trả `NULL`
      cho nhóm 3. Cả hai model dùng chung macro này.
- [x] `dbt/models/silver/silver_tickets.sql` — lọc trước (`where normalize_priority(...)
      is not null`), `row_number` sau.
- [x] `dbt/models/silver/quarantine_tickets.sql` — `where normalize_priority(...) is null`.
- [x] `dbt/models/silver/schema.yml` — `enforced: true`, thêm `tests: [not_null,
      accepted_values [1,2,3,4]]` ở cột `priority`.

Kết quả: `dbt test` ✓ 11/11, `priority ∈ 1..4 không NULL` ✓, `quarantine_tickets`
= 312, `ỔN ĐỊNH` ✓, `gold_training_set` giữ 12.480.

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
