package storesystem.middlelayer;

import Utils.Constants;
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
    LoadDataInterface loadData; // 获得headinfomation
    LoadDataInterface basicLoadData; // 获得主要的information
    LoadDataInterface appendLoadData; //获得扩展的information

    int Pos = 1;
    ArrayList<byte[]> attrAndType = new ArrayList<byte[]>();

    // 记录每种属性的取值类型0 int 1 double 2 string
    private byte[] AttrInfo;

    public byte[] getAttrAll() {
        return attrAll;
    }

    public byte[] getHeadInfo() {
        return HeadInfo;
    }

    // 记录每条记录的开始地址
    private byte[] HeadInfo;

    public int getBasicRecords() {
        return basicRecords;
    }

    private int basicRecords = 0;

    public byte[] getHeadAll() {
        return headAll;
    }

    private byte[] headAll;
    // 记录所有的属性信息
    private byte[] attrAll;

    public byte[] getDBInfo() {
        return DBInfo;
    }

    private byte[] DBInfo;

    public LoadRecords(String dataBaseName, String tableName) throws IOException {
        this.dataBaseName = dataBaseName;
        this.tableName = tableName;

        // 获得原始记录数，用以确定从哪个文件中获取数据
        gainBasicRecords(dataBaseName,tableName);

        String uri = SLSystem.getURIHead(dataBaseName, tableName);
        this.loadData = new LoadDataHDFS(uri);
        Init(); // 获得所有的头部信息
    }

    private void gainBasicRecords(String dataBaseName, String tableName) throws IOException {
        String uri = SLSystem.getURIVersion(dataBaseName,tableName);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);
        byte[] tmpBasicRecords = new byte[4];
        loadDataHDFS.read(4, tmpBasicRecords, 0, 4);
        basicRecords = SLSystem.byteArrayToInt(tmpBasicRecords, 0);
        loadDataHDFS.destroy();
    }

    private void Init() throws IOException { // 需要大改

        byte[] DBl = new byte[4];
        byte[] attrNumT = new byte[4];
        byte[] recordNumT = new byte[4];
        byte[] recordStartT = new byte[4];
        loadData.read(0, DBl,0,4);
        loadData.read(8, attrNumT, 0,4);
        loadData.read(12, recordStartT, 0,4);
        loadData.read(16, recordNumT, 0,4);

        int recordNum = SLSystem.byteArrayToInt(recordNumT,0);
        int recordStart = SLSystem.byteArrayToInt(recordStartT,0);
        int attrNum = SLSystem.byteArrayToInt(attrNumT,0);
        int DBL = SLSystem.byteArrayToInt(DBl,0);

        HeadInfo = new byte[recordNum * 4];
        AttrInfo = new byte[attrNum];

        byte[] attrS = new byte[4];
        byte[] attrE = new byte[4];
        loadData.read(20, attrS,0,4);
        loadData.read(attrNum * 4 + 20, attrE,0,4);
        int attrSI = SLSystem.byteArrayToInt(attrS,0);
        int attrEI = SLSystem.byteArrayToInt(attrE,0);

        byte[] attrLocation = new byte[attrNum * 4];

//        DBInfo = new byte[attrSI - DBL];
        attrAll = new byte[attrEI - attrSI];
        headAll = new byte[recordStart];
        loadData.read(20, attrLocation,0,attrLocation.length);
        loadData.read(recordStart, HeadInfo,0, HeadInfo.length);
        loadData.read(0, headAll, 0, headAll.length);
//        loadData.read(DBL, DBInfo, 0, DBInfo.length);
        loadData.read(attrSI, attrAll, 0, attrAll.length);
        int[] attrL = SLSystem.byteArrayToIntArray(attrLocation);
        int base = attrSI;

        for (int i = 0;i < attrL.length;i ++){
            AttrInfo[i] = attrAll[attrL[i] - base];
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
        for (int i = 0;i < tmp.length;i ++){
            tmp[i] = attrAll[i + s];
        }
        return tmp;
    }

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
        if (num * 4 >= HeadInfo.length){
            return null;
        }
        String uri = null;
        LoadDataInterface loadData = null;

        if (num < basicRecords) {
            uri = SLSystem.getURI(dataBaseName, tableName);
            int ind = (int) (num / Constants.SUBFILESIZE);
            uri = uri + ind;
        }
        else uri = SLSystem.getURIAppend(dataBaseName, tableName);

        loadData = new LoadDataHDFS(uri);

        int S ,E;
        if (num == 0) {
            S = 0;
            E = SLSystem.byteArrayToInt(HeadInfo, num * 4);
        } else {
            S = SLSystem.byteArrayToInt(HeadInfo,num * 4 - 4);
            E = SLSystem.byteArrayToInt(HeadInfo,num * 4);
        }
        byte[] res = null;
        if (E > S){
            res  = new byte[E - S];
        }
        else{
            res = new byte[E];
            S = 0;
        }
        loadData.read(S, res,0, res.length);
        StringBuffer result = new StringBuffer("");
        int offset = 0;
        for (int i = 0;i < AttrInfo.length;i ++){
            switch (AttrInfo[i]){
                case 0:
                    result.append(SLSystem.byteArrayToInt(res, offset));
                    result.append(RecordsUtils.recordSplitLabel);
                    offset += 4;
                    break;
                case 1:
                    result.append(SLSystem.byteToDouble(res, offset));
                    result.append(RecordsUtils.recordSplitLabel);
                    offset += 8;
                    break;
                case 2:
                    int len = SLSystem.byteArrayToInt(res, offset);
                    offset += 4;
                    result.append(new String(Arrays.copyOfRange(res, offset, offset + len)));
                    result.append(RecordsUtils.recordSplitLabel);
                    offset += len;
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
