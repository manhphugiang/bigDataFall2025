package projPhase2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

class dataReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> dateOutput;

    @Override
    protected void setup(Context context) {
        dateOutput = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String keyStr = key.toString(); // house_id/date - 1/2012-06-01
            String[] parts = keyStr.split("/");
            if (parts.length < 2) return;

            String date = parts[1]; // 2012-06-01
            String datePath = date.replace("-", "/"); // 2012/06/01
            String fileName = "datafile_" + date.replace("-", "_");

            dateOutput.write(key, value, datePath + "/" + fileName);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        dateOutput.close();
    }
}