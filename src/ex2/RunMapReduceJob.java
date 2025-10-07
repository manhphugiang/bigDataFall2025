package ex2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunMapReduceJob {
    public static void main(String[] args) throws Exception {
        new RunMapReduceJob().run(args);
    }

    public void run(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path  outPath = new Path(args[1]);
        System.out.println(inPath.toString());
        System.out.println(outPath.toString());
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(RunMapReduceJob.class);
        job.setJobName("Daily Average Sold");

        job.setMapperClass(orderMapper.class);
        job.setReducerClass(orderReducer.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
