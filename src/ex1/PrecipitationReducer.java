package ex1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PrecipitationReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> precipitations, Context context) throws IOException, InterruptedException {
        float totalPrec = 0.0F;
        for (FloatWritable prec: precipitations) {
            totalPrec += prec.get();
        }
        context.write(key, new FloatWritable(totalPrec));
    }
}
