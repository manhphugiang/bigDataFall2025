package projPhase1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ElectricReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private MultipleOutputs<Text, FloatWritable> dateOutput;

    @Override
    protected void setup(Context context) {
        dateOutput = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> readings, Context context) throws IOException, InterruptedException {
        float first = 0;
        float second = 0;
        int count = 0;
        for (FloatWritable reading : readings) {
            if (count == 0) {
                first = reading.get();
                count++;
            } else if (count == 1) {
                second = reading.get();
                break;
            }
        }
        float consumption = first - second; // calculate the energy consumption


        String keyStr = key.toString();  // house and date - 1/2012-06-01
        String[] parts = keyStr.split("/");

        String houseId = parts[0];       // "1"
        String date = parts[1];          // "2012-06-01"
        String datePath = date.replace("-", "/");  // "2012/06/01" - change the date format
        String fileName = "datafile_" + date.replace("-", "_");

        dateOutput.write(key, new FloatWritable(consumption), datePath + "/" + fileName);
        // time series directory structure ///Dataset/YYYY/MM/DD/datafile_YYYY_MM_DD.txt
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        dateOutput.close();
    }
}
