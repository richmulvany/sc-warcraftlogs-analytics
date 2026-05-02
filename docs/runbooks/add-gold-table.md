# Runbook: Add a New Gold Table

## Steps

### 1. Decide which source file to add it to

- **Player performance** (throughput, parse %, spec) → `pipeline/gold/player_products.py`
- **Progression/wipe analysis** → `pipeline/gold/summary_products.py`
- **Death/survivability** → `pipeline/gold/survivability_products.py`
- **Roster/profile** → `pipeline/gold/roster_products.py`
- **Profiles / exports / roster-adjacent products** → `pipeline/gold/profile_products.py`
- **Utility / wipe diagnostics** → `pipeline/gold/utility_products.py` or `pipeline/gold/wipe_diagnostics.py`
- **Mythic+ / Raider.IO** → `pipeline/gold/mplus_products.py`
- **New category** → create a new file `pipeline/gold/your_category_products.py`

### 2. Write the table function

```python
@dlt.table(
    name="03_gold.sc_analytics.gold_your_table_name",
    comment="What this table contains and what question it answers.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",  # most common join column
    },
)
def gold_your_table_name():
    # Read from fact tables or dimensions — never from silver directly
    perf = spark.read.table("03_gold.sc_analytics.fact_player_fight_performance")
    return (
        perf
        .groupBy("player_name", "player_class")
        .agg(F.count("*").alias("kills_tracked"))
        .orderBy("player_name")
    )
```

**Important patterns**:
- Always read from `fact_player_fight_performance` for performance data (not `silver_player_performance` — it lacks fight context and throughput)
- Use `dlt.read()` (batch), not `dlt.read_stream()`
- Avoid joining two DataFrames that share a non-key column name — alias or drop before joining
- Use `desc_nulls_last()` when ordering by nullable columns like `throughput_per_second`

### 3. Register in the pipeline

If it's in an existing file already registered in `databricks.yml`, no action needed.

If you created a new file, add it to `databricks.yml`:

```yaml
libraries:
  # ... existing entries ...
  - notebook:
      path: pipeline/gold/your_category_products.py
```

### 4. Deploy

```bash
databricks bundle deploy
```

The table will be created on the next pipeline run. For immediate creation:
- UI: Pipeline → Start (incremental, not full refresh)

### 5. Add contracts

Every table in `03_gold.sc_analytics` is a governed data product. Add:

- a Gold product contract in `pipeline/contracts/gold/`
- a `pipeline/contracts/data_products.yml` catalog entry
- a dashboard asset contract in `pipeline/contracts/dashboard_assets/` if the
  table is exported as static JSON

Contract fields must distinguish missing, null, and empty string semantics:

- listed fields must exist in every row
- use `nullable: true` only when `null` is meaningful and expected
- use `allowEmpty: true` only when `""` is an intentional sentinel
- keep primary key fields non-null and non-empty
- include an `exposure` value in `data_products.yml`: `dashboard`, `chatbot`,
  `internal`, or `monitoring`

### 6. Update the data dictionary

Add an entry to `docs/data_dictionary/README.md` with all column names, types, and descriptions.

### 7. Verify

```sql
-- In Databricks SQL or a notebook
SELECT COUNT(*) FROM 03_gold.sc_analytics.gold_your_table_name;
SELECT * FROM 03_gold.sc_analytics.gold_your_table_name LIMIT 10;
```

### 8. Publish to frontend

If the React frontend needs the table, add it to `EXPORT_TABLES` or
`QUERY_EXPORTS` in `scripts/publish_dashboard_assets.py`.

The preferred frontend path is manifest-driven JSON fetched from the published dashboard assets, so also add:

- A dataset key mapping if the page currently loads through `useCSV()`
- Any TypeScript row interface the consuming page needs
- Any page/component wiring that consumes the dataset through the compatibility layer or `dashboardDataClient`
- Contract metadata via `pipeline/contracts/data_products.yml`

If you need a temporary local fallback for development, `scripts/dev/export_gold_tables.py` still exists, but it is no longer the primary production publishing path.
