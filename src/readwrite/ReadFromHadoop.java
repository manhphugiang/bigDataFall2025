package readwrite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class ReadFromHadoop {
    public void readFromFile(String uri) throws Exception{
        Configuration conf = new Configuration();
        String file = uri+"/test/mytest.txt";
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path filePath = new Path(file);

        InputStream in = fs.open(filePath);
        OutputStream out = System.out;
        IOUtils.copyBytes(in, out, 4096, true);  in.close();
        out.close();

    }
}