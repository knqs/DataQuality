package storesystem.middlelayer;

import java.io.IOException;
import java.util.List;

/**
 * 加载记录，从指定的数据库表中中加载记录
 */
public interface LoadRecordsInterface {
    public String getRecord() throws IOException;
    public String getRecords(int num) throws IOException;
    public List<String> getAttrsAndType();
}
