-- Enable Recursive Scanning
SET mapreduce.input.fileinputformat.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.exec.compress.output=false;

-- Define Tables

-- Electricity Data
DROP TABLE IF EXISTS electricity_data;
CREATE EXTERNAL TABLE electricity_data (
    log_id STRING,
    house_id INT,
    con_date STRING,
    con_time STRING,
    reading FLOAT,
    flag INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/project/inputdata/dataset';

-- House Information (Comma-separated)
DROP TABLE IF EXISTS house_info;
CREATE EXTERNAL TABLE house_info (
    house INT,
    first_reading STRING,
    last_reading STRING,
    cover FLOAT,
    house_type STRING,
    facing STRING,
    region STRING,
    rus INT,
    evs INT,
    sn STRING,
    hvac STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
LOCATION '/project/inputdata/house_info'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Combined Weather Data
DROP TABLE IF EXISTS weather_all;
CREATE EXTERNAL TABLE weather_all (
    weather_date STRING,
    hour INT,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    weather STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/project/inputdata/weather'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 3. Process Weather Data (Distinguish YVR vs WYJ by filename)
DROP VIEW IF EXISTS weather_daily_processed;
CREATE VIEW weather_daily_processed AS
SELECT 
    weather_date, 
    AVG(temperature) as avg_temp,
    -- Check filename to assign region
    CASE 
        WHEN INPUT__FILE__NAME LIKE '%YVR%' THEN 'YVR'
        WHEN INPUT__FILE__NAME LIKE '%WYJ%' THEN 'WYJ'
        ELSE 'UNKNOWN' 
    END AS region
FROM weather_all
GROUP BY weather_date, INPUT__FILE__NAME;

-- Find the most recent 30 dates
DROP TABLE IF EXISTS recent_dates;
CREATE TABLE recent_dates AS
SELECT DISTINCT con_date
FROM electricity_data
ORDER BY con_date DESC
LIMIT 30;

-- Execute Final Join
-- Stored in /user/hive-output/recent30days
INSERT OVERWRITE DIRECTORY '/user/hive-output/recent30days'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    e.house_id,
    e.con_date,
    (MAX(e.reading) - MIN(e.reading)) AS daily_consumption,
    w.avg_temp
FROM electricity_data e
JOIN recent_dates rd ON e.con_date = rd.con_date
JOIN house_info h ON e.house_id = h.house
-- Join weather matching BOTH Date and Region
LEFT JOIN weather_daily_processed w 
    ON e.con_date = w.weather_date 
    AND h.region = w.region
GROUP BY e.house_id, e.con_date, w.avg_temp;