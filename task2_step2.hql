-- Task 2 Step 2: Add temperature to consumption data

-- Create table from step 1 output
DROP TABLE IF EXISTS consumption_with_region;

CREATE EXTERNAL TABLE consumption_with_region (
    house_id INT,
    con_date STRING,
    daily_consumption FLOAT,
    region STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive-output/recent30days_temp';

-- Create external table for pre-aggregated daily weather
DROP TABLE IF EXISTS weather_daily;

CREATE EXTERNAL TABLE weather_daily (
    weather_date STRING,
    region STRING,
    avg_temp FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/weather-daily'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Step 2: Join with weather
INSERT OVERWRITE DIRECTORY '/user/hive-output/recent30days'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    c.house_id,
    c.con_date,
    c.daily_consumption,
    w.avg_temp
FROM consumption_with_region c
JOIN weather_daily w ON c.con_date = w.weather_date AND c.region = w.region;

-- Create final table
DROP TABLE IF EXISTS recent30days_consumption;

CREATE EXTERNAL TABLE recent30days_consumption (
    house_id INT,
    con_date STRING,
    daily_consumption FLOAT,
    avg_temp FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive-output/recent30days';

-- Verify results
SELECT * FROM recent30days_consumption LIMIT 50;
