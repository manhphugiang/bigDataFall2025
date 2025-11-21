package projPhase2;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class dataMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header line
        if (key.get() == 0 && value.toString().contains("LOG_ID")) {
            return;
        }

        String[] values = value.toString().split("\\t");
        if (values.length >= 3) {
            String houseId_condate = values[1] + "/" + values[2]; // house_id/date
            context.write(new Text(houseId_condate), value);
        }
    }
}