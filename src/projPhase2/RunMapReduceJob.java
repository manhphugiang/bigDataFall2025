package projPhase2;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Main Job Class
public class RunMapReduceJob {
    public static void main(String[] args) throws Exception {
        new RunMapReduceJob().run(args);
    }

    public void run(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(RunMapReduceJob.class);
        job.setJobName("Task 1: Consumption Data Loader to Phase 1 Structure");
        job.setMapperClass(dataMapper.class);
        job.setReducerClass(dataReducer.class);
        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
