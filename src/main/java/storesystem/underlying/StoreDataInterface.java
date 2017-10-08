package storesystem.underlying;

import java.io.IOException;
import java.util.List;

public interface StoreDataInterface {
    /**
     * store all data including heads.
     * @param datas
     * @return
     */
    public boolean storeData(List<String> datas) throws IOException;

    /**
     * add data in the end of a special database table.
     * @param data
     * @return
     */
    public boolean addData(String data) throws IOException;
}
