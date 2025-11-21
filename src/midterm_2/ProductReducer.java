package midterm_2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProductReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count++; //get the sum
        }
        if (count > 0) {
            context.write(key, new FloatWritable(sum / count)); //get the average price
        }
    }
}
