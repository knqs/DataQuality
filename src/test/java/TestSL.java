import Utils.DataFormatException;
import storesystem.middlelayer.LoadRecords;
import storesystem.middlelayer.StoreRecords;

import java.io.IOException;
import java.util.ArrayList;

public class TestSL {
    public static void main(String... args){
        StoreRecords storeRecords = new StoreRecords();
        try {
//            storeRecords.storeRecords("C:/Users/kongn/Documents/test2.txt", "KNQ", "KNQtable");
            storeRecords.appendRecords("6 \"No.6\" 6.0 null", "KNQ", "KNQtable");
            LoadRecords loadRecords = new LoadRecords("KNQ", "KNQtable");
            String str;
            System.out.println("Records : ");
//            while((str = loadRecords.getRecord()) != null){
//                System.out.println(str);
//            }
            System.out.println(loadRecords.getNRecord(6));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
    }
}
