package readwrite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;


public class WriteToHadoop {
    public void writeToHadoop(String uri) throws Exception{


        Configuration conf = new Configuration();
        String file = uri+"/test/mytest.txt";
        FileSystem fs = FileSystem.get(URI.create(file), conf);

        Path filePath = new Path(file);

        String s = "I am streaming this sentence to a hadoop file\n";
        InputStream in = new ByteArrayInputStream(s.getBytes());
        FSDataOutputStream out = fs.create(filePath, () ->
                System.out.println("/"));
        IOUtils.copyBytes(in, out, 4096, true);
        in.close();
        out.close();
    }
}