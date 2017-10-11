package Utils;

import java.util.ArrayList;
import java.util.List;

public class SLSystem {
    // 根据数据库和表名获得文件地址
    public static String getURI(String databaseName, String tableName){
        return "hdfs://localhost:9000/database_" + databaseName + '/' + tableName;
    }

    //int转字节数组
    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte)((i >> 24) & 0xFF);
        result[1] = (byte)((i >> 16) & 0xFF);
        result[2] = (byte)((i >> 8) & 0xFF);
        result[3] = (byte)(i & 0xFF);
        return result;
    }

    // 字节数组转int
    public static int byteArrayToInt(byte[] b, int offset) {
        int value= 0;
        for (int i = 0; i < 4; i++) {
            int shift= (4 - 1 - i) * 8;
            value +=(b[i + offset] & 0x000000FF) << shift;
        }
        return value;
    }

    //浮点到字节转换
    public static byte[] doubleToByte(double d) {
        long value = Double.doubleToRawLongBits(d);
        byte[] byteRet = new byte[8];
        for (int i = 0; i < 8; i++) {
            byteRet[i] = (byte) ((value >> 8 * i) & 0xff);
        }
        return byteRet;
    }

    //字节到浮点转换
    public static double byteToDouble(byte[] arr, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i + offset] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }

    // 字节数组转字节集合
    public static List<Byte> byteArrayToCollection(byte[] tmp) {
        ArrayList<Byte> arrayList = new ArrayList<>();
        for (int i = 0;i < tmp.length;i ++){
            arrayList.add(tmp[i]);
        }
        return arrayList;
    }

    // 字节集合转字节数组
    public static byte[] byteCollectionToArray(List<Byte> list){
        byte[] bytes = new byte[list.size()];
        for (int i = 0;i < list.size();i ++){
            bytes[i] = list.get(i);
        }
        return bytes;
    }

    // 字节数组转int数组
    public static int[] byteArrayToIntArray(byte[] bytes){
        int[] result = new int[bytes.length / 4];
        for (int i = 0;i < result.length;i ++){
            result[i] = byteArrayToInt(bytes,i * 4);
        }
        return result;
    }
}
