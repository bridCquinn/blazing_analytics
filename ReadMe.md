# Key Architectural Decisions

* **High-Speed Bulk Ingestion:** Bypassed slow row-by-row `INSERT` statements by combining Go concurrency primitives (`sync.WaitGroup`, channels) with PostgreSQL's binary `COPY` protocol (`pgx/v5`). This enables multi-gigabyte CSV processing in seconds.
* **Pre-Aggregated Analytics:** Avoided expensive runtime calculations over millions of rows by using PostgreSQL Materialized Views (`mv_model_failure_rates`). Refreshed concurrently (`REFRESH MATERIALIZED VIEW CONCURRENTLY`) to ensure zero read-locks.
* **Framework-Free REST API:** Built entirely on Go’s optimized standard library (`net/http`) to eliminate dependency bloat and guarantee lightning-fast request routing.
* **Flexible S.M.A.R.T. Mapping:** Mapped optional or vendor-specific S.M.A.R.T. telemetry attributes to Go pointers (`*int64`, `*int`) so missing database values serialize correctly as JSON `null` rather than misleading zero-values.

---

# Technical Considerations & Safeguards

* **Connection Management:** Enforced strict connection pool parameters (`pgxpool` limits and `database/sql` lifetime caps) to prevent database exhaustion.
* **Indexing Strategy:** Applied compound B-tree indexes (`idx_metrics_model_failures`) and unique view indexes to accelerate filtering and support safe concurrent refreshes.
* **Fault Tolerance:** Designed ingestion workers to gracefully skip malformed CSV rows without crashing batch pipelines, paired with standard REST error responses (`400`, `404`, `500`).
