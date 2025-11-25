package projPhase3Task1A;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HourlyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (value == null) return;
        
        String line = value.toString();
        if (line == null || line.trim().isEmpty()) return;

        String[] fields = line.split("\\t");
        if (fields == null || fields.length < 7) return;

        try {
            String house = fields[2];
            String date = fields[3];
            String time = fields[4];
            String readingStr = fields[5];

            if (house == null || date == null || time == null || readingStr == null) return;
            if (time.length() < 2) return;

            float reading = Float.parseFloat(readingStr);
            String hour = time.substring(0, 2);
            String keyOut = house + "/" + date + "/" + hour;

            context.write(new Text(keyOut), new FloatWritable(reading));
        } catch (Exception e) {
            // skip
        }
    }
}

