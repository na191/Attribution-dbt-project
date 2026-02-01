# Marketing Multi-Touch Attribution Pipeline
**dbt + Snowflake**

A full dbt project that models a multi-touch marketing attribution pipeline, calculating revenue credit across five attribution models and producing ROAS dashboards.

---

## Architecture

```
Raw Data (Snowflake)          dbt Layers                          Output
─────────────────────         ──────────                          ──────
raw_events.csv      →    staging (views)       →  intermediate   →  marts (tables)
conversions.csv     →    stg_raw_events            int_user_          mart_attributed_revenue
ad_spend.csv        →    stg_conversions           touchpoints        mart_roas_by_channel
                         stg_ad_spend
```

## Attribution Models

| Model | Logic |
|---|---|
| **First Touch** | 100% credit to the first channel in the journey |
| **Last Touch** | 100% credit to the final channel before conversion |
| **Linear** | Equal credit split across all touchpoints (1/N) |
| **Time Decay** | Exponential decay with 7-day half-life; normalized per user |
| **U-Shaped** | 40% first, 40% last, 20% split among middle touchpoints |

All five models are computed in parallel in `mart_attributed_revenue`, so you can compare them side-by-side — which is the actual analytical value of this kind of project.

---

## Repo Structure

```
marketing-attribution-dbt/
├── .gitignore
├── README.md
├── dbt_project.yml              # Project config, materialization strategy
├── data/                        # Source data (load these into Snowflake)
│   ├── generate_dummy_data.py   # Reproducible script that generates the CSVs
│   ├── raw_events.csv           # 7,035 marketing touchpoints
│   ├── conversions.csv          # 2,000 users (~360 converted)
│   └── ad_spend.csv             # 90 days × 4 paid channels
├── macros/
│   └── attribution_weights.sql  # Reusable macro for all 5 weight calculations
└── models/
    ├── staging/
    │   ├── schema.yml           # Sources, tests, column docs
    │   ├── stg_raw_events.sql   # Cast + clean raw events
    │   ├── stg_conversions.sql  # Filter to converted users
    │   └── stg_ad_spend.sql     # Clean spend + derive CPM
    ├── intermediate/
    │   └── int_user_touchpoints.sql  # Join events→conversions, rank journey
    └── marts/
        ├── schema.yml
        ├── mart_attributed_revenue.sql  # Revenue credit by all 5 models
        └── mart_roas_by_channel.sql     # Join to spend → ROAS/CPA/CPM
```

> **Note:** `profiles.yml` is in `.gitignore` and is not committed. Create your own locally — see setup instructions below.

---

## Setup & Running

### 1. Install dbt

```bash
pip install dbt-snowflake
```

### 2. Create your profiles.yml

Create a `profiles.yml` file in the repo root (it's gitignored, so it stays local):

```yaml
marketing_attribution:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'ANALYST') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'MARKETING_DB') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'ANALYTICS_WH') }}"
      schema: raw
      threads: 2
      query_tag: dbt_attribution
```

Set your environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="ANALYST"
export SNOWFLAKE_DATABASE="MARKETING_DB"
export SNOWFLAKE_WAREHOUSE="ANALYTICS_WH"
```

### 3. Load data into Snowflake

```sql
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.raw_events (
    session_id VARCHAR,
    user_id VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    channel VARCHAR,
    campaign VARCHAR,
    device VARCHAR,
    page_visited VARCHAR,
    session_duration_seconds INTEGER
);

CREATE TABLE raw.conversions (
    user_id VARCHAR,
    converted BOOLEAN,
    conversion_timestamp TIMESTAMP_NTZ,
    order_value FLOAT
);

CREATE TABLE raw.ad_spend (
    spend_date DATE,
    channel VARCHAR,
    campaign VARCHAR,
    spend_usd FLOAT,
    impressions INTEGER
);
```

Load the CSVs from the `data/` folder via Snowflake UI or SnowSQL:

```sql
PUT file://data/raw_events.csv @~/;
COPY INTO raw.raw_events FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
-- Repeat for conversions.csv and ad_spend.csv
```

### 4. Run dbt

```bash
dbt debug         # Verify Snowflake connection
dbt run           # Execute all models
dbt test          # Run schema tests
dbt docs generate # Generate documentation site
dbt docs serve    # View docs locally
```

---

## Key Design Decisions

**Macro for attribution weights** — All five weight formulas live in one reusable macro (`attribution_weights.sql`). This keeps the mart model clean and makes it trivial to add new models (e.g., data-driven) later.

**Time-decay normalization** — Raw time-decay weights don't sum to 1, so normalization happens via a window function in the mart. This is a common gotcha worth highlighting in an interview.

**Pre-conversion filtering** — `int_user_touchpoints` drops any events that occurred *after* conversion. This prevents post-purchase email confirmations or retargeting clicks from inflating attribution.

**Full outer join on spend** — Organic and direct channels have zero spend. The ROAS mart uses a full outer join so these channels still appear (with null ROAS) rather than being silently dropped.

**Staging as views, marts as tables** — Staging models are lightweight casts/filters, so views are fine. Marts are the query targets for dashboards, so they materialize as tables.

---

## What to Demo / Discuss

- The **macro** pattern and why it's cleaner than repeating CASE logic
- The **normalization step** for time-decay (shows you understand the math, not just the SQL)
- The **pre-conversion filter** in the intermediate layer (data quality thinking)
- The **side-by-side ROAS comparison** — the real business question is "which model do we trust?"
- How you'd **extend this**: add data-driven attribution, add funnel-stage granularity, or wire it to a BI tool
