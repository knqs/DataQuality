package storesystem.middlelayer;

import Utils.SLSystem;
import storesystem.underlying.StoreDataHDFS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class StoreRecords implements StoreRecordsInterface {

    @Override
    public boolean storeRecords(String src, String database, String table) throws IOException {
        String dst = SLSystem.getURI(database,table);
        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(dst);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(src));
        String str;
        ArrayList<String> arrayList = new ArrayList<>();
        while((str = bufferedReader.readLine()) != null){
            arrayList.add(str + "\r\n");
        }
        storeDataHDFS.storeData(arrayList);
        return true;
    }

    @Override
    public boolean appendRecords(String record, String database, String table) throws IOException {
        String dst = SLSystem.getURI(database,table);
        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(dst);
        storeDataHDFS.addData(record);
        return true;
    }
}