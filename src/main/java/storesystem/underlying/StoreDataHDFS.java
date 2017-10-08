package storesystem.underlying;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class StoreDataHDFS implements StoreDataInterface {

    String uri;
    private FileSystem fileSystem;
    private FSDataOutputStream fsDataOutputStream;

    public StoreDataHDFS(String uri){
        this.uri = uri;
        try {
            fileSystem = FileSystem.get(URI.create(this.uri), new Configuration());
        } catch (IOException e) {
            System.err.print("Error at StoreDataHDFS...");
            e.printStackTrace();
        }
    }

    public boolean storeData(List<String> datas) throws IOException {
        fsDataOutputStream = fileSystem.create(new Path(uri));
        datas.forEach(data -> {
            try {
                fsDataOutputStream.writeBytes(data);
            } catch (IOException e) {
                System.err.print("Error at StoreData...");
                e.printStackTrace();
            }
        });
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        return true;
    }

    public boolean addData(String data) throws IOException {
        fsDataOutputStream = fileSystem.append(new Path(uri));
        fsDataOutputStream.writeBytes(data);
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        return true;
    }
}
