package projPhase3Task1A;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class HourlyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (key.get() == 0 && value.toString().contains("LOG_ID")) return;

        String[] values = value.toString().split("\\t");

        try {
            String houseId = values[1];
            String date = values[2];
            String time = values[3]; // e.g., 00:00:10
            String readingStr = values[4];

            // extract just the Hour (00 to 23)
            String hour = time.split(":")[0];

            // key: HouseID_Date_Hour (e.g., 1_2015-05-30_09)
            String mapKey = houseId + "_" + date + "_" + hour;
            float energy = Float.parseFloat(readingStr);

            context.write(new Text(mapKey), new FloatWritable(energy));

        } catch (Exception e) {
            // ignore exception
        }
    }
}