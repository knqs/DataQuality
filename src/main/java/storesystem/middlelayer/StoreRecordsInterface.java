package storesystem.middlelayer;

import java.io.IOException;

public interface StoreRecordsInterface {
    public boolean storeRecords(String src, String database, String table) throws IOException;
    public boolean appendRecords(String record, String database, String table) throws IOException;
}
