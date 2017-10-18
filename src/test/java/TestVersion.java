import Utils.DataFormatException;
import storesystem.middlelayer.LoadRecords;
import storesystem.middlelayer.StoreRecords;
import storesystem.middlelayer.VersionCtl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestVersion {
    public static void main(String... args){
        StoreRecords storeRecords = new StoreRecords();
        try {
            storeRecords.storeRecords("C:/Users/kongn/Documents/test2.txt", "KNQ", "KNQtable");
            VersionCtl versionCtl = new VersionCtl();
            versionCtl.initVersion("KNQ","KNQtable",4);
            versionCtl.createNewVersion(0,"KNQ","KNQtable");
//            versionCtl.createNewVersion(1,"KNQ","KNQtable");
            versionCtl.addRecord(1,"6 \"No.6\" 6.0 null","KNQ","KNQtable");
            versionCtl.addRecord(0,"7 \"No.7\" 7.0 null","KNQ","KNQtable");
            versionCtl.removeRecord(1,1,"KNQ","KNQtable");
            versionCtl.replaceRecord(1,3,"5 \"No.5\" 5.0 null","KNQ","KNQtable");
            int V = versionCtl.mergeVersion(0,1,"KNQ","KNQtable");
            List<String> arrayList = versionCtl.getAllData(1,"KNQ","KNQtable");
            System.out.println("Records : ");
            arrayList.forEach(data -> {
                System.out.println(data);
            });
            arrayList = versionCtl.getAllData(0,"KNQ","KNQtable");
            System.out.println("Records : ");
            arrayList.forEach(data -> {
                System.out.println(data);
            });
            arrayList = versionCtl.getAllData(V,"KNQ","KNQtable");
            System.out.println("Records : ");
            arrayList.forEach(data -> {
                System.out.println(data);
            });
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
    }
}
