package projPhase3Task1B;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DailyAvgReducer extends Reducer<Text, Text, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        Map<String, Float[]> dailyMap = new HashMap<>();

        for (Text val : values) {
            String[] parts = val.toString().split("\\t");
            String date = parts[0];
            float reading = Float.parseFloat(parts[1]);

            if (!dailyMap.containsKey(date)) {
                // First reading found for this date: initialize Min and Max as this reading
                dailyMap.put(date, new Float[]{reading, reading});
            } else {
                Float[] minMax = dailyMap.get(date);
                // Update Min
                if (reading < minMax[0]) minMax[0] = reading;
                // Update Max
                if (reading > minMax[1]) minMax[1] = reading;
            }
        }

        float totalConsumption = 0;
        int dayCount = 0;

        // Calculate consumption for each day and add to total
        for (Float[] minMax : dailyMap.values()) {
            float dailyUsage = minMax[1] - minMax[0]; // Consumption = End - Start
            totalConsumption += dailyUsage;
            dayCount++;
        }

        // Calculate Average
        float avgDailyConsumption = (dayCount > 0) ? (totalConsumption / dayCount) : 0.0f;

        // Output: HouseID, AverageDailyConsumption
        context.write(key, new FloatWritable(avgDailyConsumption));
    }
}