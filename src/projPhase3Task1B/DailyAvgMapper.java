package projPhase3Task1B;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DailyAvgMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. Skip Header
        if (key.get() == 0 && value.toString().contains("LOG_ID")) return;

        // 2. Split CSV
        String[] values = value.toString().split("\\t");

        try {
            // Indexes based on PDF: Log_ID(0), House_ID(1), ConDate(2), ConTime(3), Reading(4)
            String houseId = values[1];
            String date = values[2];
            String readingStr = values[4];

            // 3. Output Key: HouseID
            // 4. Output Value: Date + "," + Reading
            // We need the date in the value to group readings by day in the Reducer
            context.write(new Text(houseId), new Text(date + "," + readingStr));

        } catch (Exception e) {
            // Handle bad records
        }
    }
}