import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PrecipitationReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> precipitations, Context context) throws IOException, InterruptedException {
        float maxPrec = 0.0F;
        for (FloatWritable prec: precipitations) {
            maxPrec = Math.max(maxPrec, prec.get());
        }
        context.write(key, new FloatWritable(maxPrec));
    }
}
