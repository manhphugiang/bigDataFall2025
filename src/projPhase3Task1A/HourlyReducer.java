package projPhase3Task1A;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class HourlyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float minVal = Float.MAX_VALUE;
        float maxVal = Float.MIN_VALUE;

        // find Start (min) and End (max) readings for this hour
        for (FloatWritable val : values) {
            float f = val.get();
            if (f < minVal) minVal = f;
            if (f > maxVal) maxVal = f;
        }

        // calculate actual consumption (Cumulative Difference)
        float consumption = maxVal - minVal;

        // output: House_Date_Hour, Consumption
        context.write(key, new FloatWritable(consumption));
    }
}