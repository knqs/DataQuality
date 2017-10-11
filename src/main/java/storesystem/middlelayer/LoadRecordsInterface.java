package storesystem.middlelayer;

import Utils.DataFormatException;

import java.io.IOException;
import java.util.List;

/**
 * 加载记录，从指定的数据库表中中加载记录
 */
public interface LoadRecordsInterface {
    public String getRecord() throws IOException, DataFormatException;
    public String getRecords(int num) throws IOException, DataFormatException;
    public String getNRecord(int num) throws IOException, DataFormatException;
    public List<byte[]> getAttrsAndType();
}
