import Utils.DataFormatException;
import storesystem.middlelayer.StoreRecords;
import storesystem.middlelayer.VersionCtl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TestStore {
    public static void main(String... args){
        StoreRecords storeRecords = new StoreRecords();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        try {
            System.out.println(simpleDateFormat.format(new Date()));
            storeRecords.storeRecords("/home/kongning/文档/数据质量/testdata.txt", "KNQ", "KNQtable");
            System.out.println(simpleDateFormat.format(new Date()));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
    }
}
