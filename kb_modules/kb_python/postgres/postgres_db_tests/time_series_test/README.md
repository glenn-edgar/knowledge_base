# TimescaleDB Objects - Quick Reference

## Finding Your Tables in psql

When you connect to your database with psql, TimescaleDB tables may not show up with standard `\dt` commands because:
1. Hypertables are special tables with metadata stored separately
2. They might be in a different schema
3. Continuous aggregates are materialized views, not regular tables

### Method 1: Use the provided SQL script
```bash
psql -h localhost -p 5434 -U gedgar -d postgres -f find_timescale_objects.sql
```

### Method 2: Direct queries in psql

```sql
-- See all hypertables
SELECT hypertable_schema, hypertable_name 
FROM timescaledb_information.hypertables;

-- See all continuous aggregates  
SELECT view_schema, view_name, hypertable_name
FROM timescaledb_information.continuous_aggregates;

-- See regular tables
\dt

-- See materialized views
\dm
```

### Method 3: Use psql meta-commands
```sql
-- Show all tables (including hypertables)
\dt+

-- Show all materialized views (continuous aggregates are materialized views)
\dm+

-- Show table details
\d+ test_timeseries
```

## What the Updated Python Code Does

### New Features:
1. **is_hypertable(table_name)** - Check if a table is already a hypertable
2. **table_exists(table_name)** - Check if a table exists
3. **drop_table(table_name, cascade=False)** - Drop tables cleanly
4. **drop_continuous_aggregate(agg_name, cascade=False)** - Drop continuous aggregates
5. **list_hypertables()** - List all hypertables in the database
6. **list_continuous_aggregates()** - List all continuous aggregates

### Improved Behavior:
- **create_hypertable()** now has `if_not_exists=True` parameter (default)
- Test code now cleans up before running
- Shows existing objects before and after tests
- Better error messages and organization

## Running the Fixed Code

```bash
python timeseries_manager_corrected.py
```

The code will now:
1. Show existing hypertables and continuous aggregates
2. Clean up any previous test runs
3. Create new test objects
4. Run all tests
5. Show final state

## Manual Cleanup (if needed)

If you need to manually clean up:

```sql
-- Drop the continuous aggregate first (it depends on the hypertable)
DROP MATERIALIZED VIEW IF EXISTS test_cagg CASCADE;

-- Then drop the hypertable
DROP TABLE IF EXISTS test_timeseries CASCADE;
```

## Common Issues

### Issue: "Table already exists as hypertable"
**Solution**: The updated code now checks before creating and skips if exists.
Alternatively, manually drop with: `DROP TABLE test_timeseries CASCADE;`

### Issue: "Cannot see tables with \dt"
**Solution**: Use TimescaleDB information schema queries (see above) or try `\dt+`

### Issue: Continuous aggregate is empty
**Solution**: Call `refresh_continuous_aggregate()` or wait for the policy to run:
```python
ts_manager.refresh_continuous_aggregate('test_cagg')
```

Or in SQL:
```sql
CALL refresh_continuous_aggregate('test_cagg', NULL, NULL);
```

