package storesystem.underlying;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * 实现从存储层直接获得数据
 */
public class LoadDataHDFS implements LoadDataInterface {
    String uri;
    private FileSystem fileSystem;
    private FSDataInputStream fsDataInputStream;

    public LoadDataHDFS(String uri){
        this.uri = uri;
        try {
            fileSystem = FileSystem.get(URI.create(this.uri), new Configuration());
            fsDataInputStream = fileSystem.open(new Path(this.uri));
        } catch (IOException e) {
            System.out.println("Error at LoadDataHDFS...");
            e.printStackTrace();
        }
    }

    protected void finalize() throws java.lang.Throwable{
        destroy();
    }


    public void destroy() throws IOException {
        fsDataInputStream.close();
    }

    public double readDouble() throws IOException {
        return fsDataInputStream.readDouble();
    }

    public int readInt() throws IOException {
        return fsDataInputStream.readInt();
    }

    public String readLine() throws IOException {
        return fsDataInputStream.readLine();
    }

    public int read() throws IOException {
        return fsDataInputStream.read();
    }

    public int read(byte[] buffer) throws IOException {
        return fsDataInputStream.read(buffer);
    }

    public int read(byte[] buffer, int offset, int length) throws IOException {
        return fsDataInputStream.read(buffer,offset,length);
    }

    public static void main(String... args) throws IOException {
        String uri = "hdfs://localhost:9000/";
        String duri = "/home/test1.txt";
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);

        FSDataInputStream in = fs.open(new Path(duri));
        String str;
        while ((str = in.readLine()) != null){
            System.out.println(str);
        }
    }
}
