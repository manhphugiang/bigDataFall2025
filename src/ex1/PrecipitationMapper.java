package ex1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PrecipitationMapper
        extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String year;
        float prec;
        try {
            year =  values[3].substring(0,4);
            prec = Float.parseFloat(values[6]);
        }
        catch (Exception e){
            year = "9999";
            prec = 0.0F;
        }
        context.write(new Text(year), new FloatWritable(prec));
    }
}