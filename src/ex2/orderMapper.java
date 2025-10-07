package ex2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class orderMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String date;
        String amountQty = "";

        if (values[0].equals("OrderID")) {
            return;
        }


        try {
            date=  values[1];
            amountQty = values[2] + "," + values[3];
        }
        catch (Exception e){
            date = "9999";
            amountQty = ("0,0");
        }
        context.write(new Text(date), new Text(amountQty));
    }
}