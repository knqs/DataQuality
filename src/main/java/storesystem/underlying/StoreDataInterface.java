package storesystem.underlying;

import Utils.DataFormatException;

import java.io.IOException;
import java.util.List;

public interface StoreDataInterface {
    /**
     * store all data including heads.
     * @param datas
     * @return
     */
    public boolean storeData(List<byte[]> datas) throws IOException, DataFormatException;

    /**
     * add data in the end of a special database table.
     * @param data
     * @return
     */
    public boolean addData(byte[] data) throws IOException;
}
