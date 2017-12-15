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
        return HeadInfo.get(HeadInfo.size() - 1);
    }

    // 记录每条记录的开始地址
    private ArrayList<byte[]> HeadInfo = new ArrayList<>();

    public int getBasicRecords() {
        return basicRecords;
    }

    private int basicRecords = 0;
    private int totalRecords = 0;

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
        getTotalRecords(dataBaseName, tableName);
        int ind = (totalRecords - 1) / Constants.SUBHEADSIZE;
        String uri = SLSystem.getURIHead(dataBaseName, tableName);
        this.loadData = new LoadDataHDFS(uri + 0);
        Init(); // 获得所有的头部信息
        for (int i = 0;i <= ind;i ++){
            this.loadData = new LoadDataHDFS(uri + i);
            getHeadInfos();
        }
        getHeadAlls();
    }

    private void getTotalRecords(String dataBaseName, String tableName) throws IOException {
        String uri = SLSystem.getURITotalRecords(dataBaseName,tableName);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);
        byte[] tmpBasicRecords = new byte[4];
        loadDataHDFS.read(0, tmpBasicRecords, 0, 4);
        totalRecords = SLSystem.byteArrayToInt(tmpBasicRecords, 0);
        loadDataHDFS.destroy();
    }

    private void gainBasicRecords(String dataBaseName, String tableName) throws IOException {
        String uri = SLSystem.getURIVersion(dataBaseName,tableName);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);
        byte[] tmpBasicRecords = new byte[4];
        loadDataHDFS.read(4, tmpBasicRecords, 0, 4);
        basicRecords = SLSystem.byteArrayToInt(tmpBasicRecords, 0);
        loadDataHDFS.destroy();
    }

    private void getHeadInfos() throws IOException {
        byte[] recordNumT = new byte[4];
        byte[] recordStartT = new byte[4];
        loadData.read(12, recordStartT, 0,4);
        int recordStart = SLSystem.byteArrayToInt(recordStartT,0);
        loadData.read(16, recordNumT, 0,4);
        int recordNum = SLSystem.byteArrayToInt(recordNumT,0);
        recordNum = (recordNum - 1) % Constants.SUBHEADSIZE + 1;

        byte[] tmpHeadInfo = new byte[recordNum * 4 + 4];
        loadData.read(recordStart, tmpHeadInfo, 0, tmpHeadInfo.length);
        HeadInfo.add(tmpHeadInfo);
    }

    private void getHeadAlls() throws IOException {
        byte[] recordStartT = new byte[4];
        loadData.read(12, recordStartT, 0,4);
        int recordStart = SLSystem.byteArrayToInt(recordStartT,0);
        headAll = new byte[recordStart];
        loadData.read(0, headAll, 0, headAll.length);
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
        int recordStart = SLSystem.byteArrayToInt(recordStartT,0);

        int attrNum = SLSystem.byteArrayToInt(attrNumT,0);
        int DBL = SLSystem.byteArrayToInt(DBl,0);

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
        loadData.read(20, attrLocation,0,attrLocation.length);
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
            result.append(getNRecord(Pos ++));
            result.append("\n");
        }
        return new String(result);
    }

    @Override
    public String getNRecord(int num) throws IOException, DataFormatException {
        num --; // 索引从0开始，所以第一条记录的使用的实际num为0
        if (num >= totalRecords) return null;

        String uri = null;
        LoadDataInterface loadData = null;

        if (num < basicRecords) {
            uri = SLSystem.getURI(dataBaseName, tableName);
            /// 不划分文件存储数据
//            int ind = (int) (num / Constants.SUBFILESIZE);
//            uri = uri + ind;
        }
        else uri = SLSystem.getURIAppend(dataBaseName, tableName);

        loadData = new LoadDataHDFS(uri);

        int index = num / Constants.SUBHEADSIZE;
//        if (num >= basicRecords){
//            index = basicRecords / Constants.SUBHEADSIZE;
//        }
        num = num % Constants.SUBHEADSIZE ;
        //num = num % Constants.SUBHEADSIZE;// 找到它在当前头文件中的位置

        int S ,E;
        if ((num  + 2) * Constants.INDEXLENGTH > HeadInfo.get(index).length) return null; // 越界，返回空
        S = SLSystem.byteArrayToInt(HeadInfo.get(index),num * Constants.INDEXLENGTH);
        E = SLSystem.byteArrayToInt(HeadInfo.get(index),(num + 1) * Constants.INDEXLENGTH);

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
