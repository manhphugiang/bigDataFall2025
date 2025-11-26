SET hive.exec.compress.output=false;

-- House Information
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
LOCATION '/phase3-dataset-house-info';

-- Weather Data
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
LOCATION '/phase3-dataset-weather';

-- Process Weather by Region
DROP TABLE IF EXISTS weather_daily;
CREATE TABLE weather_daily AS
SELECT 
    weather_date, 
    AVG(temperature) as avg_temp,
    CASE 
        WHEN INPUT__FILE__NAME LIKE '%YVR%' THEN 'YVR'
        WHEN INPUT__FILE__NAME LIKE '%WYJ%' THEN 'WYJ'
    END AS region
FROM weather_all
WHERE INPUT__FILE__NAME LIKE '%YVR%' OR INPUT__FILE__NAME LIKE '%WYJ%'
GROUP BY weather_date, 
    CASE 
        WHEN INPUT__FILE__NAME LIKE '%YVR%' THEN 'YVR'
        WHEN INPUT__FILE__NAME LIKE '%WYJ%' THEN 'WYJ'
    END;

-- Final Output with Weather
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
LEFT JOIN weather_daily w ON e.con_date = w.weather_date AND h.region = w.region
GROUP BY e.house_id, e.con_date, w.avg_temp
ORDER BY e.house_id, e.con_date;