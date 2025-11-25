-- Task 2: Most recent 30 days daily consumption (simplified without weather)
-- Creates table: recent30days_consumption

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

-- Find the most recent 30 dates
DROP TABLE IF EXISTS recent_dates;

CREATE TABLE recent_dates AS
SELECT DISTINCT con_date
FROM electricity_data
ORDER BY con_date DESC
LIMIT 30;

-- Output to HDFS directory
INSERT OVERWRITE DIRECTORY '/user/hive-output/recent30days'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    e.house_id,
    e.con_date,
    MAX(e.reading) - MIN(e.reading) AS daily_consumption
FROM electricity_data e
JOIN recent_dates rd ON e.con_date = rd.con_date
GROUP BY e.house_id, e.con_date;

-- Create external table from output
DROP TABLE IF EXISTS recent30days_consumption;

CREATE EXTERNAL TABLE recent30days_consumption (
    house_id INT,
    con_date STRING,
    daily_consumption FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive-output/recent30days';

-- Verify results
SELECT * FROM recent30days_consumption LIMIT 50;
