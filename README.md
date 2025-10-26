# End-to-End Azure Data Engineering Project
## End-to-end Databricks Lakehouse (DLT): Medallion, streaming+batch, Expectations, Auto-CDC, SCD2, business MV.


## Description

This repository delivers an end-to-end Databricks Lakehouse built entirely with Lakeflow Declarative Pipelines (DLT). It implements the Medallion pattern—Bronze landing with Expectations, Silver enrichment via Auto-CDC (Type-1 upserts) exposed through stable views, and Gold with SCD2 dimensions, a Type-1 fact table, and a full-history materialized business view. The project supports streaming and batch in one pipeline, comes with reusable utilities, and provides SQL scripts to seed/increment data plus screenshots of each stage.



## Bronze — Ingestion & Quality Gate (DLT Streaming)

**Purpose.** Land raw operational data (Sales East/West, Products, Customers) as append-only Delta tables, preserving fidelity while unifying batch + streaming into a single, incremental pipeline.

**How it’s built.**
- Empty streaming landing tables are created (`sales_stg`, `products_stg`, `customers_stg`) with `dlt.create_streaming_table`.
- `dlt.append_flow` funnels **both** `sales_east` and `sales_west` into **one** canonical `sales_stg`, simplifying lineage and avoiding per-region output tables.
- Sources are read with `spark.readStream.table(...)` for streaming inputs (sales) and `spark.read.table(...)` where batch is appropriate; DLT automatically processes only new data.

**Data quality (Expectations).**
- Row-level rules via `@dlt.expect_all_or_drop` / `@dlt.expect_all` (e.g., non-null keys, positive quantities, valid statuses).  
- Actions are configured to **drop** bad rows at source or **warn/fail** the flow; counts show up in the “Expectations” pane and event logs for audit.

**Schema handling.**
- Additive schema changes are tolerated (e.g., new optional columns); strictness is enforced with Expectations so upstream drift doesn’t corrupt downstream layers.

**Idempotency & replay.**
- Checkpointing/state is handled by DLT; reruns are safe and **incremental**.  
- The repo includes SQL seed + incremental scripts so you can validate that Bronze processes only the newly inserted records.

**Why Bronze avoids dedup/business logic.**
- Bronze remains **append-only and raw** to preserve traceability; deduping, SCD logic, and business rules are deferred to Silver/Gold.

 <img src="https://github.com/pninad9/DataBricks-Declarative-Pipelines/blob/a86f076ad67bc10ebaf76697201f3aa6272bf664/ScreenShot/Bronze.png" />

### Silver — Cleaned, Upserted & Modeled (DLT with Auto-CDC)

**Purpose.** Convert raw Bronze streams into clean, deduplicated, **upserted** Delta tables (Type-1 behavior) with consistent types and business-ready columns that downstream layers can trust.

**How it’s built.**
- For each domain (Sales, Products, Customers), an **empty streaming target** is created (e.g., `sales_enriched`) via `dlt.create_streaming_table`.
- `dlt.create_auto_cdc_flow` performs **upserts** (merge semantics) from the Bronze staging streams:
  - `keys=[...]` define business/natural keys (e.g., `sale_id`, `product_id`, `customer_id`).
  - `sequence_by=...` (event/modified timestamp) ensures the **latest** version wins.
  - Optional flags like `ignore_null_updates`, `apply_as_deletes` keep state coherent in late/partial updates.
- Transformations (renames, type casting, computed fields like `total_amount = quantity * unit_price`) are done **before** the upsert so the stored Silver is clean.

**Row-level quality.**
- Expectations continue in Silver (e.g., non-null keys, domain checks); non-conforming rows are dropped or flagged so bad data never reaches Gold.

**Why we read via a **View** (not the Silver table).**
- Silver **tables mutate** (upserts) by design. Direct streaming reads from changing sources can be fragile or force full reprocessing.
- A **streaming view** (e.g., `sales_enriched_v`) sits on top of the Silver table to:
  - provide a **stable projection/contract** (only the columns and types Gold needs),
  - enforce **incremental filtering** for downstream reads,
  - decouple Gold from physical changes (renames, added columns) in the upserted table.
- This same view is reused in Gold so the last hop stays **purely incremental** without re-reading historical data.

**Idempotency & lineage.**
- Auto-CDC keeps only the latest version per key; re-runs and late arrivals converge to the same state.
- The DLT graph shows clear dependencies from Bronze staging → Silver view/table → Gold consumers, making audits straightforward.

<img src="https://github.com/pninad9/DataBricks-Declarative-Pipelines/blob/a86f076ad67bc10ebaf76697201f3aa6272bf664/ScreenShot/silver.png" />


###  Gold — Curated Dimensional Model & Business Views (DLT + SCD Type-2)

**Purpose.** Publish analytics-ready data: **Type-2 dimensions** (history preserved), an **upserted fact**, and **business views** that BI tools can query with zero prep.

**How it’s built.**
- Sources are the **Silver streaming views** (not the mutating tables) to keep the last hop purely incremental and schema-stable.
- Dimensions (e.g., `dim_products`, `dim_customers`) use **DLT SCD-Type-2** semantics with:
  - natural `keys=[...]` (e.g., `product_id`, `customer_id`) and `sequence_by=<last_updated_ts>` for version ordering,
  - surrogate keys, `effective_start`, `effective_end`, and `is_current` columns maintained by the pipeline.
- Fact table (e.g., `fact_sales`) is an **upserted** Delta table (Type-1 behavior) keyed by the business transaction id; late/changed events are merged in place.

**Transformations.**
- Conformed columns and types from Silver (e.g., computed `total_amount`) are retained.
- Star-schema joins are aligned on surrogate/natural keys to support rollups (region, category) and time slicing.

**Why we read via a **View** from Silver (not the Silver table).**
- Silver tables **mutate** due to upserts; streaming directly from them can be brittle or force reprocessing.
- A **streaming view** on Silver provides a stable projection and clean incremental feed for Gold, decoupling Gold from storage-level changes.

**Why a **Materialized View** in Gold (for business views).**
- The **business view** (e.g., `business_sales_by_region`) is delivered as a **materialized view** so it reflects the **entire historical dataset** (full picture), not just the incremental slice you’d get from a streaming read.
- Materialization persists aggregated results, giving predictable performance and point-in-time correctness for dashboards.

**Idempotency & lineage.**
- Re-runs converge: SCD-2 manages version history; fact upserts reconcile late events; materialized views rebuild deterministically.
- The DLT graph shows Bronze → Silver (views) → Gold (dims/facts) → Business MV lineage for easy audits.

- ![Gold layer]
<img src="https://github.com/pninad9/DataBricks-Declarative-Pipelines/blob/a86f076ad67bc10ebaf76697201f3aa6272bf664/ScreenShot/gold.png" />
- ![Business view]
<img src= "https://github.com/pninad9/DataBricks-Declarative-Pipelines/blob/a86f076ad67bc10ebaf76697201f3aa6272bf664/ScreenShot/business%20view.png" />

###  Final Run
The pipeline runs incrementally, not as a full refresh. Bronze append flows ingest only new events; Silver uses auto-CDC upserts (keys, sequence_by) so unchanged rows are skipped; Gold applies SCD Type-2 so only natural-key rows with actual changes produce new versions (old versions are closed, new ones opened). Metrics like “written”/“upserted” therefore reflect just the delta of this run (e.g., 4 new sales + 2 customer updates), not the full table size. Additionally, any records failing DLT expectations (data-quality rules) are dropped or warned, further reducing the count for that run.
- ![After final run]
<img src="https://github.com/pninad9/DataBricks-Declarative-Pipelines/blob/a86f076ad67bc10ebaf76697201f3aa6272bf664/ScreenShot/after%20final%20run.png" />

### Final Words

End-to-end Databricks Lakehouse build using DLT: incremental Bronze (append flow), Silver (auto-CDC upserts via streaming views), and Gold (SCD-2 dims + fact) with materialized business views for full-history analytics. Views act as stable contracts between layers; expectations enforce data quality; code is modular and production-ready. It demonstrates practical mastery of batch+stream unification, CDC, and dimensional modeling in a modern Lakehouse.and Databricks Asset Bundles. I also built reusable Python utilities so new tables become plug-and-play. Bottom line: I can design, implement, and operate a modern Azure data platform governed, testable, and ready to serve analytics with real-world SLAs bringing day-one impact to your team.
