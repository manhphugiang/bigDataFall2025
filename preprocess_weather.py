#!/usr/bin/env python3
"""
Pre-aggregate weather data from hourly to daily averages
"""

import sys
from collections import defaultdict

def process_weather_file(input_file, output_file, region):
    """Aggregate hourly weather data to daily averages"""
    
    daily_temps = defaultdict(list)
    
    # Read and aggregate
    with open(input_file, 'r') as f:
        # Skip header
        next(f)
        
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 3:
                continue
            
            date = parts[0]
            try:
                temp = float(parts[2])
                daily_temps[date].append(temp)
            except (ValueError, IndexError):
                continue
    
    # Write daily averages
    with open(output_file, 'w') as f:
        f.write("date,region,avg_temp\n")
        for date in sorted(daily_temps.keys()):
            avg_temp = sum(daily_temps[date]) / len(daily_temps[date])
            f.write(f"{date},{region},{avg_temp:.2f}\n")
    
    print(f"Processed {len(daily_temps)} days from {input_file}")

if __name__ == "__main__":
    # Process YVR
    process_weather_file(
        '/home/pi/data/dataset/Weather_YVR.txt',
        '/home/pi/weather_yvr_daily.csv',
        'YVR'
    )
    
    # Process WYJ
    process_weather_file(
        '/home/pi/data/dataset/Weather_WYJ.txt',
        '/home/pi/weather_wyj_daily.csv',
        'WYJ'
    )
    
    print("Weather data aggregated successfully!")
    print("Files created:")
    print("  /home/pi/weather_yvr_daily.csv")
    print("  /home/pi/weather_wyj_daily.csv")
