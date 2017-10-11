package storesystem.underlying;

import java.io.IOException;

/**
 * 面向于数据存储直接接触的一层，根据指定的文件名获得文件内容
 */
public interface LoadDataInterface {
    public double readDouble() throws IOException;
    public int readInt() throws IOException;
    public String readLine() throws IOException;
    public int read() throws IOException;
    public int read(byte[] buffer) throws IOException;
    public int read(byte[] buffer, int offset, int length) throws IOException;
    public int read(long position, byte[] buffer, int offset, int length) throws IOException;
}
