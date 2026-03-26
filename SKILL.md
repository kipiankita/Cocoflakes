---
name: data-usage-patterns-analysis
description: Analyze data usage and access patterns from query history to recommend archival, transient table conversions, and storage cost savings. Triggers include data usage analysis, access patterns, archival recommendations, transient tables, storage savings, cost optimization, query history analysis, usage patterns presentation.
---

# Data Usage Patterns Analysis

Analyze query history to produce table/view-level usage insights, archival recommendations, storage cost savings, and presentation talking points.

**CRITICAL:**
- Never hardcode database/schema/table/view names — discover dynamically from role grants.
- Use `SSNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` as the primary query history source. `SSNOWFLAKE.ACCOUNT_USAGE` contains **tables only** (no views).
- Use `SNOWFLAKE.ACCOUNT_USAGE` only as fallback for TABLE_STORAGE_METRICS.

## Workflow

### Step 1: List Accessible Objects

Run `SHOW DATABASES`, then for each user database (skip SNOWFLAKE, SSNOWFLAKE, SNOWFLAKE_SAMPLE_DATA): `SHOW SCHEMAS IN DATABASE <db>`, then for each schema (skip INFORMATION_SCHEMA): `SHOW TABLES IN <db>.<schema>` and `SHOW VIEWS IN <db>.<schema>`.

Present inventory: | Database | Schema | Object Name | Object Type | Rows | Bytes |

### Step 2: Grant Missing Access

If user mentions objects NOT in Step 1 inventory, provide GRANTs:
```sql
GRANT USAGE ON DATABASE <db> TO ROLE <role>;
GRANT USAGE ON SCHEMA <db>.<schema> TO ROLE <role>;
GRANT SELECT ON ALL TABLES IN SCHEMA <db>.<schema> TO ROLE <role>;
GRANT SELECT ON ALL VIEWS IN SCHEMA <db>.<schema> TO ROLE <role>;
```
Re-run Step 1 after grants. **STOP: Confirm missing objects are now visible before proceeding.**

### Step 3: Confirm Analysis Scope

**ASK THE USER:** Which databases, schemas, tables, and views to analyze, and what time range.
Present Step 1+2 inventory as options. **STOP: Confirm final list and time range before proceeding.**

### Step 4: Check ACCOUNT_USAGE Access

Test: `SELECT 1 FROM SSNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY LIMIT 1;`
Test: `SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS LIMIT 1;`
SSNOWFLAKE access is **required** for Step 6.

### Step 5: Grant ACCOUNT_USAGE Access (if needed)

```sql
USE ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA SSNOWFLAKE.ACCOUNT_USAGE TO ROLE <role>;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <role>;
```
Re-run Step 4 to confirm.

### Step 6: Analyze Query History

Query `SSNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` for user-specified time range.

**6a. Schema-level frequency:** GROUP BY DATABASE_NAME, SCHEMA_NAME — count queries, distinct users, bytes scanned, earliest/latest query.

**6b. Object-level access via QUERY_TEXT:** For each confirmed table/view, search `UPPER(QUERY_TEXT) LIKE '%<OBJECT_NAME>%'` to find query_count, distinct_users, first_access, last_access. Objects with zero matches were not queried.

**6c. Query type breakdown:** GROUP BY DATABASE_NAME, SCHEMA_NAME, QUERY_TYPE.

### Step 7: Data Time-Range Usage Per Table

For each confirmed **table**: `DESCRIBE TABLE` to find date/timestamp columns, then query `MIN(<date_col>), MAX(<date_col>)` for total data range. Compare with queried window from Step 6b to determine archivable range. Note: views inherit data range from base tables.

### Step 8: Archival Recommendations

Get storage from `SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS` (fall back to SHOW TABLES bytes). Ask user for storage rate (default $40/TB/month).

**Output these exact columns per table** — generate a document link the user can open to view the full output:

| Column | Description |
|--------|-------------|
| a. Total Size (GB) | Total billed bytes converted to GB |
| b. Monthly Storage Cost ($) | (total_GB / 1024) * rate_per_TB |
| c. Archivable Years (from — to) | Date range of archivable data |
| d. Archivable Size (GB) | Bytes of archivable data in GB |
| e. Archivable % | archivable_GB / total_GB * 100 |
| f. Monthly Savings ($) | (archivable_GB / 1024) * rate_per_TB |
| g. Monthly Savings (%) | monthly_savings / monthly_cost * 100 |
| h. Yearly Savings ($) | monthly_savings * 12 |
| i. Yearly Savings (%) | yearly_savings / yearly_cost * 100 |

Categorize: **ARCHIVE** (not queried or has data older than queried range), **REVIEW** (queried < 5 times), **KEEP** (actively queried). Flag views dependent on archived tables.

### Step 9: Transient Table Candidates

From confirmed tables, identify TRANSIENT candidates based on: delete_to_insert_ratio > 0.5, staging/ETL naming (STG, TMP, TEMP, RAW, LANDING), short-lived data. Check IS_TRANSIENT from storage metrics. Output: table name, current IS_TRANSIENT, FAILSAFE_BYTES, criteria met.

### Step 10: Aggregated Cost Savings

Produce **3 aggregation levels** using all Step 8 columns **except column c** (Archivable Years):

**Aggregation 1 — Per Schema:**
| Database | Schema | Total Size (GB) | Monthly Cost ($) | Archivable Size (GB) | Archivable % | Monthly Savings ($) | Monthly Savings (%) | Yearly Savings ($) | Yearly Savings (%) |

**Aggregation 2 — Per Database:**
| Database | Total Size (GB) | Monthly Cost ($) | Archivable Size (GB) | Archivable % | Monthly Savings ($) | Monthly Savings (%) | Yearly Savings ($) | Yearly Savings (%) |

**Aggregation 3 — All Databases (Grand Total):**
| Total Size (GB) | Monthly Cost ($) | Archivable Size (GB) | Archivable % | Monthly Savings ($) | Monthly Savings (%) | Yearly Savings ($) | Yearly Savings (%) |

### Step 11: Presentation Talking Points (5 Slides)

**STOP: Present all findings to user for approval before finalizing.**

**Slide 1 — Executive Summary:** Objects analyzed, time period, total storage footprint, current monthly cost, headline savings estimate.

**Slide 2 — Usage Heatmap & Time-Range Analysis:** Per-table/view query counts, distinct users, last access. Per-table total data range vs. actively queried range, archivable windows.

**Slide 3 — Archival Recommendations:** Tables recommended for ARCHIVE with sizes, savings, risk level. Views impacted by archival (dependency warnings).

**Slide 4 — Transient Table Recommendations:** Candidates with criteria met, fail-safe savings, compliance considerations.

**Slide 5 — Cost Savings Summary (ALL 3 AGGREGATIONS FROM STEP 10):** Per-schema aggregation, per-database aggregation, grand total aggregation. Monthly/yearly savings in $ and %.

## Stopping Points

1. **After Step 2** — Confirm user can see all required objects
2. **After Step 3** — Confirm analysis scope (databases/schemas/tables/views and time range)
3. **After Step 8** — Review archival recommendations before proceeding
4. **After Step 11** — Approve presentation talking points

## Output Artifacts

- **Archival recommendations document** (Step 8 table with document link)
- **Cost savings summary** with 3 aggregation levels (Step 10)
- **Presentation script** with data-backed talking points for 5 slides
- **SQL queries** saved to workspace for reproducibility

## Notes

- SSNOWFLAKE.ACCOUNT_USAGE contains **tables** not views. Use `GRANT SELECT ON ALL TABLES`.
- QUERY_TEXT is truncated at 100K chars; long queries may miss object references.
- Transient tables: 0-1 day Time Travel, NO Fail-safe. Views cannot be transient.
- Storage pricing: On-Demand ~$40/TB/month, Capacity ~$23/TB/month. Always confirm with user.
- 1 TB = 1,099,511,627,776 bytes. Views have no independent storage cost.
