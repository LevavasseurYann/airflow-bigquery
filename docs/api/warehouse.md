# warehouse

One interface, two backends. The pipeline talks to a `Warehouse`, never to
DuckDB or BigQuery directly. `get_warehouse()` picks the concrete implementation
from `Settings`, so swapping local ↔ production is a pure configuration change.

```python
with get_warehouse(settings) as wh:
    wh.load_dataframe("raw_hr", "employees", df)
```

!!! warning "DuckDB concurrency"
    DuckDB allows many concurrent readers but only **one writer**. Open
    read-only connections with `read_only=True` for query-only tasks so they
    don't contend for the writer lock.

---

::: hr_pipeline.warehouse.WarehouseSettings

---

::: hr_pipeline.warehouse.Warehouse

---

::: hr_pipeline.warehouse.DuckDBWarehouse

---

::: hr_pipeline.warehouse.BigQueryWarehouse

---

::: hr_pipeline.warehouse.get_warehouse
