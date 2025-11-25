# Hive Tasks - Final Scripts

## Prerequisites

1. Upload data to HDFS (run once):
```bash
./upload_data_to_hdfs.sh
```

## Task 1: April 2015 Daily Consumption

Creates table with each housing unit's total daily electricity consumption for each day in April 2015.

**Run:**
```bash
/opt/hive/bin/hive -f task1_april2015_final.hql
```

**Output Table:** `april2015_daily_consumption`
- Columns: house_id, con_date, daily_consumption
- Location: `/user/hive-output/april2015`

**View Results:**
```bash
/opt/hive/bin/hive -e "SELECT * FROM april2015_daily_consumption;"
```

## Task 2: Most Recent 30 Days with Temperature

Creates table with each housing unit's total daily electricity consumption for the most recent 30 days, including average daily temperature for that housing unit's region.

**Run:**
```bash
/opt/hive/bin/hive -f task2_recent30days_final.hql
```

**Output Table:** `recent30days_consumption`
- Columns: house_id, con_date, daily_consumption, avg_temp
- Location: `/user/hive-output/recent30days`

**View Results:**
```bash
/opt/hive/bin/hive -e "SELECT * FROM recent30days_consumption LIMIT 50;"
```

## Files

- `task1_april2015_final.hql` - Task 1 script
- `task2_recent30days_final.hql` - Task 2 script
- `upload_data_to_hdfs.sh` - Upload house info and weather data
- `HIVE_INSTRUCTIONS_UPDATED.md` - Detailed instructions
