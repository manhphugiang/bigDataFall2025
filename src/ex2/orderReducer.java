package ex2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class orderReducer extends Reducer<Text, Text, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float totalAmount = 0;
        float totalQty = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            float amount = Float.parseFloat(parts[0]);
            float qty = Float.parseFloat(parts[1]);
            totalAmount += amount;
            totalQty += qty;
        }

        if (totalQty != 0) {
            float average = totalAmount / totalQty;
            context.write(key, new FloatWritable(average));
        }
    }
}
