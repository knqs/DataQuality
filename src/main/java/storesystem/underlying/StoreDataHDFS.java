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
import java.util.Scanner;

public class StoreDataHDFS implements StoreDataInterface {

    String uri;
    private FileSystem fileSystem;
    private FSDataOutputStream fsDataOutputStream;

    public StoreDataHDFS(String uri) throws IOException {
        this.uri = uri;
        fileSystem = FileSystem.get(URI.create(this.uri), new Configuration());
    }

    /**
     * 数据库名	表名	属性数目	记录开始地址  记录数目	属性1名和取值类型	属性2名和取值类型	属性3名和取值类型	…	记录1	记录2
     Location1	Location2	Num1	Num2    Num3	Location3	Location4	Location5	...	 Location6	Location7

     * @param datas
     * @return
     * @throws IOException
     * @throws DataFormatException
     */
    public boolean storeData(List<byte[]> datas) throws IOException, DataFormatException {
        checkExist();
        datas.forEach(data -> {
            try {
                fsDataOutputStream.write(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        fsDataOutputStream.flush();
        System.out.println();
        return true;
    }

    public boolean storeData(byte[] data) throws IOException, DataFormatException {
        checkExist();
        fsDataOutputStream.write(data);
        fsDataOutputStream.flush();
        return true;
    }

    private void checkExist() {
        if (fsDataOutputStream == null || fileSystem == null){
            try {
                fsDataOutputStream = fileSystem.create(new Path(uri), new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print(".");
                    }
                });
            } catch (IOException e) {
                System.err.print("Error at StoreDataHDFS...");
                e.printStackTrace();
            }
        }
    }

    public boolean close() throws IOException {
        if (fsDataOutputStream != null)
            fsDataOutputStream.close();
        return true;
    }

    @Override
    public boolean addData(byte[] data) throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(uri));
        List<byte[]> list = new ArrayList<>();
        byte[] tmp = new byte[1024];
        int len = 0;
        while ((len = fsDataInputStream.read(tmp)) != -1){
            if (len == tmp.length)
                list.add(tmp);
            else{
                byte[] tmp1 = new byte[len];
                System.arraycopy(tmp, 0, tmp1, 0, tmp1.length);
                list.add(tmp1);
            }
        }
        list.add(data);
        fsDataInputStream.close();
        FSDataOutputStream fsO = fileSystem.create(new Path(uri));
        list.forEach(datas -> {
            try {
                fsO.write(datas);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        fsO.close();
        return true;
    }

    public void clean() throws IOException {
        fileSystem.delete(new Path(uri), false);
        fileSystem.create(new Path(uri)).close();
    }

}
