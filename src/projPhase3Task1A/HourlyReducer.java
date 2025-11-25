package projPhase3Task1A;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class HourlyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private float globalMax = 0.0f;
    private String globalKey = "";

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;

        for (FloatWritable v : values) {
            float reading = v.get();
            if (reading > max) {
                max = reading;
            }
            if (reading < min) {
                min = reading;
            }
        }

        float hourlyConsumption = max - min;
        
        if (hourlyConsumption > globalMax) {
            globalMax = hourlyConsumption;
            globalKey = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        context.write(new Text(globalKey), new FloatWritable(globalMax));
    }
}
