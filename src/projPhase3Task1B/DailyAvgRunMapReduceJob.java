package projPhase3Task1B;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DailyAvgRunMapReduceJob {
    public static void main(String[] args) throws Exception {
        new DailyAvgRunMapReduceJob().run(args);
    }

    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: DailyAvgRunMapReduceJob <input path> <output path>");
            System.exit(-1);
        }

        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        System.out.println("Input Path: " + inPath.toString());
        System.out.println("Output Path: " + outPath.toString());

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DailyAvgRunMapReduceJob.class);
        job.setJobName("Phase 3 Task 1B: Average Daily Consumption Analysis");

        // Set Mapper and Reducer
        job.setMapperClass(DailyAvgMapper.class);
        job.setReducerClass(DailyAvgReducer.class);

        // Map Output Types (Key=Text, Value=Text because we pass "Date,Reading")
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final Output Types (Key=Text, Value=FloatWritable)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}