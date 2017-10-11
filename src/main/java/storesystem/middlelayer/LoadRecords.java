package storesystem.middlelayer;

import Utils.DataFormatException;
import Utils.RecordsUtils;
import Utils.SLSystem;
import storesystem.underlying.LoadDataHDFS;
import storesystem.underlying.LoadDataInterface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoadRecords implements LoadRecordsInterface {

    String dataBaseName;
    String tableName;
    LoadDataInterface loadData;

    int Pos = 1;

    ArrayList<byte[]> attrAndType = new ArrayList<byte[]>();

    // 记录每种属性的取值类型0 int 1 double 2 string
    private byte[] AttrInfo;

    public byte[] getAttrAll() {
        return attrAll;
    }

    public byte[] getBodyAll() {
        return bodyAll;
    }

    // 记录每条记录的开始地址
    private byte[] HeadInfo;

    public byte[] getHeadAll() {
        return headAll;
    }

    private byte[] headAll;
    // 记录所有的属性信息
    private byte[] attrAll;
    // 记录所有的数据内容
    private byte[] bodyAll;

    public byte[] getDBInfo() {
        return DBInfo;
    }

    private byte[] DBInfo;

    public LoadRecords(String dataBaseName, String tableName) throws IOException {
        this.dataBaseName = dataBaseName;
        this.tableName = tableName;
        Init();
    }

    private void Init() throws IOException {
        String uri = SLSystem.getURI(dataBaseName, tableName);
        loadData = new LoadDataHDFS(uri);

        byte[] DBl = new byte[4];
        byte[] attrNumT = new byte[4];
        byte[] recordNumT = new byte[4];
        loadData.read(0,DBl,0,4);
        loadData.read(8, attrNumT, 0,4);
        loadData.read(12, recordNumT, 0,4);
        int recordNum = SLSystem.byteArrayToInt(recordNumT,0);
        int attrNum = SLSystem.byteArrayToInt(attrNumT,0);
        int DBL = SLSystem.byteArrayToInt(DBl,0);
        HeadInfo = new byte[recordNum * 4 + 4];
        AttrInfo = new byte[attrNum];
//        System.out.println("recordNum = " + recordNum + "attrNum = " + attrNum);

        byte[] attrS = new byte[4];
        byte[] attrE = new byte[4];
        byte[] bodyE = new byte[4];
        loadData.read(16,attrS,0,4);
        loadData.read(attrNum * 4 + 16, attrE,0,4);
        loadData.read(attrNum * 4 + recordNum * 4 + 16, bodyE,0,4);
        int attrSI = SLSystem.byteArrayToInt(attrS,0);
        int attrEI = SLSystem.byteArrayToInt(attrE,0);
        int bodyEI = SLSystem.byteArrayToInt(bodyE,0);
        byte[] attrLocation = new byte[attrNum * 4];

        DBInfo = new byte[attrSI - DBL];
        attrAll = new byte[attrEI - attrSI];
        bodyAll = new byte[bodyEI - attrEI];
        headAll = new byte[(5 + attrNum + recordNum) * 4];
//        System.out.println("attrEI = " + attrEI + "attrSI = " + attrSI);
        loadData.read(16, attrLocation,0,attrLocation.length);
        loadData.read((attrNum + 4) * 4, HeadInfo,0, HeadInfo.length);
        loadData.read(0,headAll,0,headAll.length);
        loadData.read(DBL,DBInfo,0,DBInfo.length);
        loadData.read(attrSI, attrAll,0,attrAll.length);
        loadData.read(attrEI, bodyAll,0,bodyAll.length);
        int[] attrL = SLSystem.byteArrayToIntArray(attrLocation);
        int base = attrSI;
//        for (int i = 0;i < attrAll.length;i ++){
//            System.out.println("attrAll[" + i + "] = " + attrAll[i]);
//        }
        for (int i = 0;i < attrL.length;i ++){
            AttrInfo[i] = attrAll[attrL[i] - base];
//            System.out.println("AttrInfo[" + i + "] = " + AttrInfo[i]);
            byte[] tmp;
            if (i < attrL.length - 1)
                tmp = getPartByte(attrAll,attrL[i] - base,attrL[i+1] - base);
            else
                tmp = getPartByte(attrAll,attrL[i] - base, attrAll.length);
            attrAndType.add(tmp);
        }
    }

    private byte[] getPartByte(byte[] attrAll, int s, int e) {
        byte[] tmp = new byte[e - s];
//        System.out.println("s = " + s + " e " + e + " attrAll.length = " + attrAll.length);
        for (int i = 0;i < tmp.length;i ++){
            tmp[i] = attrAll[i + s];
        }
        return tmp;
    }

//    private void getAttributes() throws IOException {
//        String str;
//        String[] strs;
//        while(true){
//            str = loadData.readLine();
//            strs = str.split(RecordsUtils.headSplitLabel);
//            if(strs[0].toLowerCase().equals("@data")){
//                break ;
//            }
//            else if(!strs[0].toLowerCase().equals("@attributes")){
//                System.err.print("DataError in Database " + dataBaseName + " and table " + tableName);
//                break;
//            }
//            else{
//                attrAndType.add(strs[1]);
//                attrAndType.add(strs[2]);
//            }
//        }
//    }
//
//    /**
//     * 检查数据库是否一致
//     */
//    private void checkDatabase() throws IOException {
//        String[] strs1, strs2;
//        String str1, str2;
//        if ((str1 = loadData.readLine()) == null){
//            System.err.print("Database Name Error!");
//        }
//        if ((str2 = loadData.readLine()) == null){
//            System.err.print("Table Name Error!");
//        }
//        strs1 = str1.split(RecordsUtils.headSplitLabel); //分隔符依据指定的数据格式
//        strs2 = str2.split(RecordsUtils.headSplitLabel);
//        if(strs1[1].equals(dataBaseName) && strs2[1].equals(tableName)){
//        } else {
//            System.err.print("Error! databaseName or tableName inconsistency!");
//        }
//    }

    @Override
    public String getRecord() throws IOException, DataFormatException {
        return getNRecord(Pos ++);
    }

    @Override
    public String getRecords(int num) throws IOException, DataFormatException {
        StringBuffer result = new StringBuffer();
        for (int i = 0;i < num;i ++){
            if (Pos * 4 >= HeadInfo.length){
                break;
            }
            else{
                result.append(getNRecord(Pos ++));
                result.append("\n");
            }
        }
        return new String(result);
    }

    @Override
    public String getNRecord(int num) throws IOException, DataFormatException {
        num --;
        if (num * 4 + 4 >= HeadInfo.length){
            return null;
        }
        int S = SLSystem.byteArrayToInt(HeadInfo,num * 4);
        int E = SLSystem.byteArrayToInt(HeadInfo,num * 4 + 4);

        byte[] res = new byte[E - S];
//        System.out.println(S + " " + E);
        loadData.read(S, res,0,res.length);
        StringBuffer result = new StringBuffer("");
        int offset = 0;
//        System.out.println("res.length = " + res.length);
        for (int i = 0;i < AttrInfo.length;i ++){
            switch (AttrInfo[i]){
                case 0:
                    result.append(SLSystem.byteArrayToInt(res, offset));
                    result.append(RecordsUtils.recordSplitLabel);
                    offset += 4;
//                    System.out.println(result);
                    break;
                case 1:
                    result.append(SLSystem.byteToDouble(res, offset));
                    result.append(RecordsUtils.recordSplitLabel);
                    offset += 8;
//                    System.out.println(result);
                    break;
                case 2:
                    int len = SLSystem.byteArrayToInt(res,offset);
                    offset += 4;
                    result.append(new String(Arrays.copyOfRange(res,offset,offset + len)));
                    result.append(RecordsUtils.recordSplitLabel);
                    offset += len;
//                    System.out.println(result);
                    break;
                default:
                    System.out.println("Data Format Error! " + AttrInfo[i] + " " + i);
                    throw new DataFormatException("Data Format Error! " + AttrInfo[i]);
            }
        }
        return new String(result);
    }

    @Override
    public List<byte[]> getAttrsAndType() {
        return attrAndType;
    }

}
