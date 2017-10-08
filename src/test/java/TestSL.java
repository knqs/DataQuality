import storesystem.middlelayer.LoadRecords;
import storesystem.middlelayer.StoreRecords;

import java.io.IOException;

public class TestSL {
    public static void main(String... args){
        StoreRecords storeRecords = new StoreRecords();
        try {
            storeRecords.storeRecords("C:/Users/kongn/Documents/test2.txt", "KNQ", "KNQtable");
            LoadRecords loadRecords = new LoadRecords("KNQ", "KNQtable");
            String str;
            while((str = loadRecords.getRecord()) != null){
                System.out.println(str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
