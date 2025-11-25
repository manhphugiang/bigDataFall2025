#!/bin/bash

# Upload house info and weather data to HDFS for Hive

echo "Creating HDFS directories..."
hadoop fs -mkdir -p /user/house-info
hadoop fs -mkdir -p /user/weather-yvr
hadoop fs -mkdir -p /user/weather-wyj

echo "Uploading House_Info.csv..."
hadoop fs -put -f /home/pi/data/dataset/House_Info.csv /user/house-info/

echo "Uploading Weather_YVR.txt..."
hadoop fs -put -f /home/pi/data/dataset/Weather_YVR.txt /user/weather-yvr/

echo "Uploading Weather_WYJ.txt..."
hadoop fs -put -f /home/pi/data/dataset/Weather_WYJ.txt /user/weather-wyj/

echo "Verifying uploads..."
hadoop fs -ls /user/house-info
hadoop fs -ls /user/weather-yvr
hadoop fs -ls /user/weather-wyj

echo "Done! Data uploaded to HDFS."
