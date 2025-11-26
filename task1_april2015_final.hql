-- Enable Recursive 
SET mapreduce.input.fileinputformat.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.exec.compress.output=false;

-- Define Electricity Table

DROP TABLE IF EXISTS electricity_data;
CREATE EXTERNAL TABLE electricity_data (
    log_id STRING,
    house_id INT,
    con_date STRING, -- Format YYYY-MM-DD
    con_time STRING,
    reading FLOAT,
    flag INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/phase3-dataset';

-- Analysis for April 2015
-- Results will be stored in /user/hive-output/april2015
INSERT OVERWRITE DIRECTORY '/user/hive-output/april2015'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    house_id, 
    con_date, 
    (MAX(reading) - MIN(reading)) AS daily_consumption
FROM electricity_data
WHERE con_date LIKE '2015-04-%'
GROUP BY house_id, con_date;