-- Task 1: Daily electricity consumption for all houses in April 2015
-- Creates table: april2015_daily_consumption

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

-- Step 1: Output to HDFS directory
INSERT OVERWRITE DIRECTORY '/user/hive-output/april2015'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    house_id,
    con_date,
    MAX(reading) - MIN(reading) AS daily_consumption
FROM electricity_data
WHERE con_date >= '2015-04-01' AND con_date <= '2015-04-30'
GROUP BY house_id, con_date;

-- Step 2: Create external table from output
DROP TABLE IF EXISTS april2015_daily_consumption;

CREATE EXTERNAL TABLE april2015_daily_consumption (
    house_id INT,
    con_date STRING,
    daily_consumption FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive-output/april2015';

-- Verify results
SELECT * FROM april2015_daily_consumption;
