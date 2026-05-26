# ingestion

Implements the **extract → land → load** pattern for regional HR source files.

```
discover_source_files()        # find all employees_*.csv in raw_data_dir
    │
    ├── extract_to_parquet()   # validate + land one file as Parquet (parallel)
    │   (one call per region)
    │
    └── load_raw_employees()   # consolidate all Parquets into raw_hr.employees
```

Steps 1–2 run in parallel (dynamic task mapping in Airflow); step 3 is
sequential because DuckDB has a single writer.

The raw layer is kept as **strings** on purpose: casting is the responsibility
of the dbt staging models, never of ingestion.

---

::: hr_pipeline.ingestion.IngestionError

---

::: hr_pipeline.ingestion.SourceFile

---

::: hr_pipeline.ingestion.RegionExtract

---

::: hr_pipeline.ingestion.IngestionResult

---

::: hr_pipeline.ingestion.discover_source_files

---

::: hr_pipeline.ingestion.extract_to_parquet

---

::: hr_pipeline.ingestion.load_raw_employees
