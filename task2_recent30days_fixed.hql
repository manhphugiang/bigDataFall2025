-- Task 2: Most recent 30 days daily consumption with average temperature

SET mapreduce.input.fileinputformat.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.exec.compress.output=false;

-- Create external table for electricity data
DROP TABLE IF EXISTS electricity_data;

CREATE EXTERNAL TABLE electricity_data (
    house_date STRING,
    log_id INT,
    house_id INT,
    con_date STRING,
    con_time STRING,
    reading FLOAT,
    flag INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/final-dataset-phase3';

-- Create external table for house information
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
LOCATION '/user/house-info'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create external table for YVR weather data
DROP TABLE IF EXISTS weather_yvr;

CREATE EXTERNAL TABLE weather_yvr (
    weather_date STRING,
    hour STRING,
    temperature FLOAT,
    humidity INT,
    pressure FLOAT,
    weather STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/weather-yvr'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create external table for WYJ weather data
DROP TABLE IF EXISTS weather_wyj;

CREATE EXTERNAL TABLE weather_wyj (
    weather_date STRING,
    hour STRING,
    temperature FLOAT,
    humidity INT,
    pressure FLOAT,
    weather STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/weather-wyj'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Find the most recent 30 dates first
DROP TABLE IF EXISTS recent_dates;

CREATE TABLE recent_dates AS
SELECT DISTINCT con_date
FROM electricity_data
ORDER BY con_date DESC
LIMIT 30;

-- Aggregate YVR weather for recent dates only
DROP TABLE IF EXISTS weather_yvr_daily;

INSERT OVERWRITE DIRECTORY '/user/hive-output/weather_yvr_daily'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    weather_date,
    AVG(temperature) AS avg_temp
FROM weather_yvr
WHERE weather_date IN (SELECT con_date FROM recent_dates)
GROUP BY weather_date;

-- Aggregate WYJ weather for recent dates only
DROP TABLE IF EXISTS weather_wyj_daily;

INSERT OVERWRITE DIRECTORY '/user/hive-output/weather_wyj_daily'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    weather_date,
    AVG(temperature) AS avg_temp
FROM weather_wyj
WHERE weather_date IN (SELECT con_date FROM recent_dates)
GROUP BY weather_date;

-- Create tables from aggregated weather
CREATE EXTERNAL TABLE weather_yvr_daily (
    weather_date STRING,
    avg_temp FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive-output/weather_yvr_daily';

CREATE EXTERNAL TABLE weather_wyj_daily (
    weather_date STRING,
    avg_temp FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive-output/weather_wyj_daily';

-- Output final result
INSERT OVERWRITE DIRECTORY '/user/hive-output/recent30days'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    e.house_id,
    e.con_date,
    MAX(e.reading) - MIN(e.reading) AS daily_consumption,
    CASE 
        WHEN h.region = 'YVR' THEN wy.avg_temp
        WHEN h.region = 'WYJ' THEN wj.avg_temp
        ELSE NULL
    END AS avg_temp
FROM electricity_data e
JOIN recent_dates rd ON e.con_date = rd.con_date
JOIN house_info h ON e.house_id = h.house
LEFT JOIN weather_yvr_daily wy ON e.con_date = wy.weather_date
LEFT JOIN weather_wyj_daily wj ON e.con_date = wj.weather_date
GROUP BY e.house_id, e.con_date, h.region, wy.avg_temp, wj.avg_temp;

-- Create external table from output
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
