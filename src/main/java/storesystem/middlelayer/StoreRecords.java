package storesystem.middlelayer;

import Utils.DataFormatException;
import Utils.RecordsUtils;
import Utils.SLSystem;
import Utils.VersCtl;
import storesystem.underlying.StoreDataHDFS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StoreRecords implements StoreRecordsInterface {

    @Override
    public boolean storeRecords(String src, String database, String table) throws IOException, DataFormatException {
        String dst = SLSystem.getURI(database,table);
        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(dst);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(src));
        String str;
        ArrayList<String> arrayList = new ArrayList<>();
        while((str = bufferedReader.readLine()) != null){
//            arrayList.add(str + RecordsUtils.recordSplitLabel + VersCtl.getVersNum() + "\r\n");
            arrayList.add(str);
        }
        storeData(storeDataHDFS, arrayList);
        return true;
    }

    private void storeData(StoreDataHDFS storeDataHDFS, ArrayList<String> datas) throws DataFormatException, IOException {
        int attrNum = 0;
        int recordNum = 0;
        byte[] DBName = datas.get(0).getBytes();
        byte[] TblName = datas.get(1).getBytes();
        ArrayList<byte[]> HeadInfo = new ArrayList<>();
        ArrayList<byte[]> AttrInfo = new ArrayList<>();
        ArrayList<byte[]> BodyInfo = new ArrayList<>();
        for (int i = 2;;i ++){
            String[] strs = datas.get(i).split(RecordsUtils.headSplitLabel);
            if (strs[0].equals("@data")){
                break ;
            }
            else if (strs[0].equals("@attributes")){
                if (strs.length != 3){
                    System.out.println(strs.length);
                    throw new DataFormatException("DataFormat Error! Attributes num is " + attrNum + 1);
                }
                if (strs[2].equals("int")){
                    AttrInfo.add(new byte[]{0});
                }
                else if (strs[2].equals("double")){
                    AttrInfo.add(new byte[]{1});
                }
                else if (strs[2].equals("string")){
                    AttrInfo.add(new byte[]{2});
                }
                else{
                    System.out.println(strs[2]);
                    throw new DataFormatException("DataFormat Error! Attributes num is " + attrNum + 1);
                }
                AttrInfo.add(strs[1].getBytes());
                attrNum ++;
            }
            else{
                throw new DataFormatException("DataFormat Error!");
            }
        }

        recordNum = datas.size() - attrNum - 3;

        int BasicLocation = 4 * (5 + recordNum + attrNum);
        int Offset = BasicLocation;
        HeadInfo.add(getBytes(Offset));
        Offset += DBName.length;
        HeadInfo.add(getBytes(Offset));
        Offset += TblName.length;
        HeadInfo.add(getBytes(attrNum));
        HeadInfo.add(getBytes(recordNum));

        HeadInfo.add(getBytes(Offset));
        for (int i = 0;i < AttrInfo.size();i += 2){
            Offset += AttrInfo.get(i + 1).length + 1;
            HeadInfo.add(getBytes(Offset));
        }


        for (int i = attrNum + 3;i < datas.size();i ++){
//            System.out.println(datas.get(i));
            byte[] tmp = getDataBytes(AttrInfo,datas.get(i));
            int len = tmp.length;
            BodyInfo.add(tmp);
            Offset += len;
            HeadInfo.add(getBytes(Offset));
        }
        HeadInfo.add(DBName);
        HeadInfo.add(TblName);
        HeadInfo.addAll(AttrInfo);
        HeadInfo.addAll(BodyInfo);

        storeDataHDFS.storeData(HeadInfo);
    }
    private byte[] getDataBytes(ArrayList<byte[]> attrInfo, String s) throws DataFormatException {
        String[] strs = s.split(RecordsUtils.recordSplitLabel);
        ArrayList<Byte> arrayList = new ArrayList<>();
        for (int i = 0;i < strs.length;i ++){
            switch (attrInfo.get(i * 2)[0]){
                case 0:
                    arrayList.addAll(arrToCol(getBytes(Integer.valueOf(strs[i]))));
                    break;
                case 1:
                    arrayList.addAll(arrToCol(getBytes(Double.valueOf(strs[i]))));
                    break;
                case 2:
                    List<Byte> list = arrToCol(strs[i].getBytes());
                    int len = list.size();
                    arrayList.addAll(arrToCol(getBytes(len)));
                    arrayList.addAll(list);
                    break;
                default:
                    throw new DataFormatException("Data Type Error! line : " + (i + 2));
            }
        }
        return colToArr(arrayList);
    }

    private byte[] colToArr(List<Byte> list){
        return SLSystem.byteCollectionToArray(list);
    }

    private List<Byte> arrToCol(byte[] bytes){
        return SLSystem.byteArrayToCollection(bytes);
    }

    private byte[] getBytes(int num){
        return SLSystem.intToByteArray(num);
    }

    private byte[] getBytes(double num){
        return SLSystem.doubleToByte(num);
    }
    @Override
    public boolean appendRecords(String record, String database, String table) throws IOException, DataFormatException {
        LoadRecords loadRecords = new LoadRecords(database,table);
        byte[] oldHead = loadRecords.getHeadAll();
        byte[] DBInfo = loadRecords.getDBInfo();
        byte[] attrAll = loadRecords.getAttrAll();
        byte[] oldBody = loadRecords.getBodyAll();

        byte[] newHead = new byte[oldHead.length + 4];

        oldHead = addN(oldHead,4);

        byte[] newrecord = getDataBytes(oldHead,attrAll,record);

        int lasInd = SLSystem.byteArrayToInt(oldHead,oldHead.length - 4);
//        System.out.println("lasInd = " + lasInd);
//        System.out.println("newrecord = " + newrecord.length);

        byte[] newLen = SLSystem.intToByteArray(newrecord.length + lasInd);
        System.arraycopy(oldHead,0,newHead,0,oldHead.length);
        System.arraycopy(newLen,0,newHead,oldHead.length,newLen.length);
        byte[] newBody = new byte[oldBody.length + newrecord.length];
        System.arraycopy(oldBody,0,newBody,0,oldBody.length);
        System.arraycopy(newrecord,0,newBody,oldBody.length,newrecord.length);

        ArrayList<byte[]> arrayList = new ArrayList<>();
        arrayList.add(newHead);
        arrayList.add(DBInfo);
        arrayList.add(attrAll);
        arrayList.add(newBody);
        String dst = SLSystem.getURI(database,table);
        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(dst);
        storeDataHDFS.storeData(arrayList);
        return true;
    }

    private byte[] addN(byte[] oldHead, int num) {
        int[] tmp = SLSystem.byteArrayToIntArray(oldHead);
        for (int i = 0;i < tmp.length;i ++){
            if (i != 2 && i != 3){
                tmp[i] += num;
            }
            else if (i == 3){
                tmp[i] += 1;
            }
        }
        return SLSystem.intArrayToByteArray(tmp);
    }

    private byte[] getDataBytes(byte[] oldHead, byte[] attrAll, String record) throws DataFormatException {
        ArrayList<byte[]> arrayList = new ArrayList<>();

        int attrNum = SLSystem.byteArrayToInt(oldHead,8);
        int base = SLSystem.byteArrayToInt(oldHead,16);
//        System.out.println(attrNum + " " + base);
        for (int i = 0;i < attrNum;i ++){
            int Slocation = SLSystem.byteArrayToInt(oldHead,16 + i * 4);
            int Elocation = SLSystem.byteArrayToInt(oldHead,20 + i * 4);
            byte[] tmp1 = Arrays.copyOfRange(attrAll,Slocation - base,Slocation - base + 1);
            byte[] tmp2 = Arrays.copyOfRange(attrAll,Slocation - base + 1,Elocation - base);
            arrayList.add(tmp1);
//            System.out.println(tmp1[0]);
            arrayList.add(tmp2);
        }
        return getDataBytes(arrayList,record);
    }
}