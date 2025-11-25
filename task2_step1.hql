-- Task 2 Step 1: Get recent 30 days consumption with house region

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

-- Find the most recent 30 dates
DROP TABLE IF EXISTS recent_dates;

CREATE TABLE recent_dates AS
SELECT DISTINCT con_date
FROM electricity_data
ORDER BY con_date DESC
LIMIT 30;

-- Step 1: Get consumption with region
INSERT OVERWRITE DIRECTORY '/user/hive-output/recent30days_temp'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    e.house_id,
    e.con_date,
    MAX(e.reading) - MIN(e.reading) AS daily_consumption,
    h.region
FROM electricity_data e
JOIN recent_dates rd ON e.con_date = rd.con_date
JOIN house_info h ON e.house_id = h.house
GROUP BY e.house_id, e.con_date, h.region;
