import Utils.SLSystem;

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

        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0;i < 10;i ++){
            set.add(i);
        }
        Integer[] res = set.toArray(new Integer[0]);
        for (int i = 0;i < res.length;i ++){
            System.out.println(res[i]);
        }
    }
}
