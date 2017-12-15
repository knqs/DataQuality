import Utils.SLSystem;
import org.apache.hadoop.fs.PositionedReadable;

import java.io.DataInputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

public class intToByte {
    public static void main(String... args){
//        int Num = 200;
//        byte[] bytes = SLSystem.intToByteArray(Num);
//        for (int i = 0;i < bytes.length;i ++){
//            System.out.println(bytes[i]);
//        }
//        int nNum = SLSystem.byteArrayToInt(bytes,0);
//        System.out.println(nNum);
//
//        double n = 1.0;
//        byte[] bytes1 = SLSystem.doubleToByte(n);
//        for (int i = 0;i < bytes.length;i ++){
//            System.out.println(bytes1[i]);
//        }
//        double nn = SLSystem.byteToDouble(bytes1,0);
//        System.out.println(nn);
        String tmp;
        RandomAccessFile F=null;
        InputStream inputStream = null;
        Reader r = null;
        byte[] a = new byte[]{1};
        System.out.println(a.length);
    }
}
