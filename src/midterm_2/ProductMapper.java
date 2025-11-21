package midterm_2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //skip header
        if (key.get() == 0 && line.contains("Prod_ID")) {
            return;
        }
        String[] values = line.split(",");
        if (values.length < 4) {
            return;
        }
        try {
            String country = values[3].trim();
            float price = Float.parseFloat(values[1]);
            context.write(new Text(country), new FloatWritable(price));
        } catch (Exception e) {
            //errors
        }
    }
}
