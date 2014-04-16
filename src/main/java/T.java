import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import java.io.EOFException;
import java.io.IOException;

public class T {
  public static void main(String[] argv) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    FSDataInputStream fileIn = fs.open(new Path("/tmp/testfile"));
    while (true) {
        try {
            int c = (int)fileIn.readUnsignedByte();
            System.out.println(String.format("Char: %d %s", c, (c == '\n' ? "*" : "")));
        } catch (EOFException e) {
            break;
        }
    }
  }
}
