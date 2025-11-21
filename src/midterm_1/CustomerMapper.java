package midterm_1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CustomerMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        //skip header
        if (values[0].equalsIgnoreCase("customer_ID") || values.length < 6) {
            return;
        }
        try {
            String city = values[4].trim();
            float creditLimit = Float.parseFloat(values[5]);
            context.write(new Text(city), new FloatWritable(creditLimit));
        } catch (Exception e) {
            //errors
        }
    }
}