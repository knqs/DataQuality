import Utils.DataFormatException;
import storesystem.middlelayer.StoreRecords;
import storesystem.middlelayer.VersionCtl;

import java.io.IOException;
import java.util.List;

public class TestStore {
    public static void main(String... args){
        StoreRecords storeRecords = new StoreRecords();
        try {
            storeRecords.storeRecords("/home/kongning/文档/数据质量/dataaffr/user_profile_sample_dealt.affr.0", "KNQ", "KNQtable");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
    }
}
