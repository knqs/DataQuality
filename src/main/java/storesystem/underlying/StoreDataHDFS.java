package storesystem.underlying;

import Utils.DataFormatException;
import Utils.RecordsUtils;
import Utils.SLSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
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

    /**
     * 数据库名	表名	属性数目	记录数目	属性1名和取值类型	属性2名和取值类型	属性3名和取值类型	…	记录1	记录2
     Location1	Location2	Num1	Num2	Location3	Location4	Location5	...	 Location6	Location7

     * @param datas
     * @return
     * @throws IOException
     * @throws DataFormatException
     */
    public boolean storeData(List<byte[]> datas) throws IOException, DataFormatException {
        fsDataOutputStream = fileSystem.create(new Path(uri), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        datas.forEach(data -> {
            try {
                fsDataOutputStream.write(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        System.out.println();

        return true;
    }

    @Override
    public boolean addData(byte[] data) throws IOException {
        return false;
    }

    /**
     * 暂时未用
     * @param data
     * @return
     * @throws IOException
     */
    public boolean addData(String data) throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(uri));
        String str;
        String tmp = "";
        while((str = fsDataInputStream.readLine()) != null){
            tmp += str + "\r\n";
        }
        fsDataInputStream.close();

        fsDataOutputStream = fileSystem.create(new Path(uri), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
        fsDataOutputStream.writeBytes(tmp);
        fsDataOutputStream.writeBytes(data);
        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        System.out.println();

        return true;
    }
}
