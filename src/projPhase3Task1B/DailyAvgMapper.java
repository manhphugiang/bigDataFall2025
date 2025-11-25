package projPhase3Task1B;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DailyAvgMapper extends Mapper<LongWritable, Text, Text, Text> {
    
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
            String readingStr = fields[5];

            if (house == null || date == null || readingStr == null) return;

            float reading = Float.parseFloat(readingStr);
            
            context.write(new Text(house), new Text(date + "," + reading));
        } catch (Exception e) {
            // skip
        }
    }
}
