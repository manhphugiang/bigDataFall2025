package midterm_1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CustomerReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> creditLimit, Context context) throws IOException, InterruptedException {
        float minLimit = Float.MAX_VALUE;
        for (FloatWritable limit : creditLimit) {
            minLimit = Math.min(minLimit, limit.get());
        }
        context.write(key, new FloatWritable(minLimit));
    }
}



//  Create a MapReduce program that will find the minimum credit limit of customers for each City.
// customer_ID,Customer_Name,membership_type,Customer_Age,Customer_City,Credit_Limit
//1,Some Name,Platinum,42,Oakville,1020.24
//        2,Some Name,Platinum,62,Milton ,2200.54
//        3,Some Name,Platinum,48,Milton ,2355.55