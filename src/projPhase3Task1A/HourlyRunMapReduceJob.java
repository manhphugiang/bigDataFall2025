package projPhase3Task1A;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import projPhase3Task1A.HourlyMapper;
import projPhase3Task1A.HourlyReducer;

public class HourlyRunMapReduceJob {
    public static void main(String[] args) throws Exception {
        new HourlyRunMapReduceJob().run(args);
    }

    public void run(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        System.out.println("Input Path: " + inPath.toString());
        System.out.println("Output Path: " + outPath.toString());

        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        Job job = Job.getInstance(conf);

        job.setJarByClass(HourlyRunMapReduceJob.class);
        job.setJobName("Phase 3 Task 1A: Max Hourly Consumption Analysis");

        // Set the Mapper and Reducer classes defined in the other files
        job.setMapperClass(HourlyMapper.class);
        job.setReducerClass(HourlyReducer.class);

        // Set the output Key/Value types (Text, FloatWritable)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // Standard Input/Output config
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}