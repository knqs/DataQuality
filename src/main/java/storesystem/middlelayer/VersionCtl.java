package storesystem.middlelayer;

import Utils.DataFormatException;
import Utils.SLSystem;
import storesystem.underlying.LoadDataHDFS;
import storesystem.underlying.StoreDataHDFS;

import java.io.IOException;
import java.util.*;

public class VersionCtl {

    public VersionCtl(){

    }

    /**
     *  获得属于一个版本的所有数据
     * @param versionNum 版本号
     * @param database 数据库名
     * @param table 表名
     * @return 获得的所有数据
     * @throws IOException
     * @throws DataFormatException
     */
    public List<String> getAllData(int versionNum, String database, String table) throws IOException, DataFormatException {
        String uri = SLSystem.getURIVersion(database,table);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);

        // 获得头部信息
        int[] headInfoI = getHeadInfo(loadDataHDFS,uri,versionNum);
        // 获得父版本信息
        ArrayList<Integer> vNums = getFaVerionInfo(versionNum,loadDataHDFS,headInfoI);
        // 获得所有记录信息
        HashSet<Integer> recordNum = getAllRecordsNum(vNums,loadDataHDFS,headInfoI);

        loadDataHDFS.destroy();

        // 获得所有记录
        uri = SLSystem.getURI(database,table);
        LoadRecords loadRecords = new LoadRecords(database,table);
        List<String> result = new ArrayList<>();
        recordNum.forEach(data -> {
            try {
                result.add(loadRecords.getNRecord(data));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (DataFormatException e) {
                e.printStackTrace();
            }
        });
        return result;
    }

    private HashSet<Integer> getAllRecordsNum(ArrayList<Integer> vNums, LoadDataHDFS loadDataHDFS, int[] headInfoI) throws IOException {
        HashSet<Integer> recordNum = new HashSet<>();
//        System.out.println("vNums.size() = " + vNums.size());
        for (int i = vNums.size() - 1;i >=0;i --){
            int vTmp = vNums.get(i);
//            System.out.println("vTmp = " + vTmp);
            int dataS = headInfoI[vTmp + 1];
            int dataE = headInfoI[vTmp + 2];
//            System.out.println("dataS = " + dataS + ",dataE = " + dataE);
            byte[] datas = new byte[dataE - dataS];
            loadDataHDFS.read(dataS,datas,0,datas.length);
            ArrayList<HashSet<Integer>> changedInfo = getChanges(datas);
            recordNum.addAll(changedInfo.get(0));
            recordNum.removeAll(changedInfo.get(1));
        }
        return recordNum;
    }

    private ArrayList<Integer> getFaVerionInfo(int versionNum, LoadDataHDFS loadDataHDFS, int[] headInfoI) throws IOException {
        ArrayList<Integer> vNums = new ArrayList<>();
        vNums.add(versionNum);
        while (true){
            int vTmp = vNums.get(vNums.size() - 1);
            int indTmp = headInfoI[vTmp + 1];
            byte[] nextV = new byte[4];
            loadDataHDFS.read(indTmp + 4,nextV,0,nextV.length);
            int nextVI = SLSystem.byteArrayToInt(nextV,0);
            if (nextVI == vTmp){
                break;
            }
            vNums.add(nextVI);
        }
        return vNums;
    }

    private int[] getHeadInfo(LoadDataHDFS loadDataHDFS, String uri, int versionNum) throws IOException, DataFormatException {
        byte[] VerNum = new byte[4];
        loadDataHDFS.read(0,VerNum,0,4);
        int VerNumI = SLSystem.byteArrayToInt(VerNum,0);
        if (versionNum >= VerNumI){
            throw new DataFormatException("versionNum not exits!");
        }

        // 获得版本信息地址
        byte[] headInfo = new byte[VerNumI * 4 + 8];
        loadDataHDFS.read(0,headInfo,0,headInfo.length);
        return SLSystem.byteArrayToIntArray(headInfo);
    }

    private ArrayList<HashSet<Integer>> getChanges(byte[] datas) {
        int[] datasI = SLSystem.byteArrayToIntArray(datas);
        int addedV = datasI[2];
        int removedV = datasI[3];
        int changedV = datas[4];
//        System.out.println(addedV + " " + removedV + " " + changedV);
        HashSet<Integer> addedHash = new HashSet<>();
        HashSet<Integer> removedHash = new HashSet<>();
        for (int i = 5;i < 5 + addedV;i ++){
            addedHash.add(datasI[i]);
        }
        for (int i = 5 + addedV;i < 5 + addedV + removedV;i ++){
            removedHash.add(datasI[i]);
        }
        for (int i = 5 + addedV + removedV;i < datasI.length;i += 2){
            addedHash.add(datasI[i]);
            removedHash.add(datasI[i + 1]);
        }
        ArrayList<HashSet<Integer>> result = new ArrayList<>();
        result.add(addedHash);
        result.add(removedHash);
        return result;
    }

    /**
     * 初始化一个版本
     * @param database 数据库名
     * @param table 表名
     * @param num 表中记录数
     * @return 成功或失败
     * @throws IOException
     * @throws DataFormatException
     */
    public boolean initVersion(String database, String table, int num) throws IOException, DataFormatException {
        String uri = SLSystem.getURIVersion(database,table);
//        FileSystem fileSystem = FileSystem.get(URI.create(uri), new Configuration());
//
//        if (fileSystem.exists(new Path(uri))){
//            throw new FileAlreadyExistsException("File Already Exists");
//        }

        int VersionNum = 1;
        int V1Location = 12;

        ArrayList<byte[]> arrayList = new ArrayList<>();
        arrayList.add(SLSystem.intToByteArray(VersionNum));
        arrayList.add(SLSystem.intToByteArray(V1Location)); // 增加版本文件头信息

        int V1 = 0;
        int preV1 = 0;
//        int addedRecord = SLSystem.byteArrayToInt(datas,12);
        int addedRecord = num;
        int removedRecord = 0;
        int replacedRecord = 0;
        arrayList.add(SLSystem.intToByteArray(V1));
        arrayList.add(SLSystem.intToByteArray(preV1));
        arrayList.add(SLSystem.intToByteArray(addedRecord));
        arrayList.add(SLSystem.intToByteArray(removedRecord));
        arrayList.add(SLSystem.intToByteArray(replacedRecord));
        for (int i = 1;i <= addedRecord;i ++){
            arrayList.add(SLSystem.intToByteArray(i));
        }

        int V2Location = V1Location + 20 + addedRecord * 4;
        arrayList.add(2,SLSystem.intToByteArray(V2Location));

        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(uri);
        storeDataHDFS.storeData(arrayList);
        return true;
    }

    /**
     *  创建一个新版本的数据库
     * @param faVersionNum 父版本号
     * @param database 数据库名
     * @param table 表名
     * @return
     * @throws IOException
     * @throws DataFormatException
     */
    public boolean createNewVersion(int faVersionNum, String database, String table) throws IOException, DataFormatException {

        String uri = SLSystem.getURIVersion(database,table);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);

        ArrayList<byte[]> newRecord = new ArrayList<>();

        byte[] num = new byte[4];
        loadDataHDFS.read(0,num,0,4);
        int VersionNum = SLSystem.byteArrayToInt(num,0);
        byte[] oldHead = new byte[VersionNum * 4 + 8];
        loadDataHDFS.read(0,oldHead,0,oldHead.length);

        byte[] tmp = getVersionBody(loadDataHDFS,0, VersionNum);

        int newVersion = VersionNum;
        int addedNum = 0;
        int removedNum = 0;
        int replacedNum = 0;
        newRecord.add(SLSystem.intToByteArray(newVersion));
        newRecord.add(SLSystem.intToByteArray(faVersionNum));
        newRecord.add(SLSystem.intToByteArray(addedNum));
        newRecord.add(SLSystem.intToByteArray(removedNum));
        newRecord.add(SLSystem.intToByteArray(replacedNum));

        byte[] endL = new byte[4]; //存储版本记录的结束位置
        loadDataHDFS.read(VersionNum * 4 + 4,endL,0,4);
        int endLI = SLSystem.byteArrayToInt(endL,0);
        byte[] newL = SLSystem.intToByteArray(endLI + 24);

//        System.out.println("oldHead = " + SLSystem.byteArrayToInt(oldHead,0));
        oldHead = addN(oldHead,4);
//        System.out.println("oldHead = " + SLSystem.byteArrayToInt(oldHead,0));
        byte[] newHead = new byte[oldHead.length + 4];
        System.arraycopy(oldHead,0,newHead,0,oldHead.length);
        System.arraycopy(newL,0,newHead,oldHead.length,newL.length);

        ArrayList<byte[]> result = new ArrayList<>();
        result.add(newHead);
        result.add(tmp);
        result.addAll(newRecord);
        loadDataHDFS.destroy();

        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(uri);
        storeDataHDFS.storeData(result);
        return true;
    }

    private byte[] getVersionBody(LoadDataHDFS loadDataHDFS, int StartVerNum,int EndVerNum) throws IOException {
        byte[] startL = new byte[4]; //存储版本记录的开始位置
        byte[] endL = new byte[4]; //存储版本记录的结束位置
        loadDataHDFS.read(4 + StartVerNum * 4,startL,0,4);
        loadDataHDFS.read(EndVerNum * 4 + 4,endL,0,4);
        int startLI = SLSystem.byteArrayToInt(startL,0);
        int endLI = SLSystem.byteArrayToInt(endL,0);
        byte[] tmp = new byte[endLI - startLI];
        loadDataHDFS.read(startLI,tmp,0,tmp.length);
        return tmp;
    }

    private byte[] addN(byte[] oldHead,int num) {
        int[] tmp = SLSystem.byteArrayToIntArray(oldHead);
        for (int i = 0;i < tmp.length;i ++){
            if (i != 0){
                tmp[i] += num;
            }
            else if (i == 0){
                tmp[i] += 1;
            }
        }
        return SLSystem.intArrayToByteArray(tmp);
    }

    /**
     * 向一个特定版本数据库中加入一条记录
     * @param versionNum 版本号
     * @param record 记录
     * @param database 数据库名
     * @param table 表名
     * @return 成功或失败标志
     * @throws IOException
     * @throws DataFormatException
     */
    public boolean addRecord(int versionNum, String record, String database, String table) throws IOException, DataFormatException {
        String uri = SLSystem.getURIVersion(database,table);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);
        int[] headInfoI = getHeadInfo(loadDataHDFS,uri,versionNum);

        StoreRecords storeRecords = new StoreRecords();
        int totalnum = storeRecords.appendRecords(record,database,table);

        int totalV = headInfoI[0];
        // 获得不同版本记录的信息
        byte[] pre = getVersionBody(loadDataHDFS,0,versionNum);
        byte[] oldV = getVersionBody(loadDataHDFS,versionNum, versionNum + 1);
//        System.out.println(versionNum + " " + totalV);
        byte[] las = getVersionBody(loadDataHDFS,versionNum + 1,totalV);

        int[] oldVI = SLSystem.byteArrayToIntArray(oldV);
        oldVI[2] ++; // 增加的记录数加一
//        System.out.println("oldVI[2] = " + oldVI[2]);
        int[] newVI = new int[oldVI.length + 1];
//        System.out.println("newvI.length = " + newVI.length);
        System.arraycopy(oldVI,0,newVI,0,oldVI[2] + 4);
        System.arraycopy(oldVI,oldVI[2] + 4,newVI,oldVI[2] + 5,oldVI.length - 4 - oldVI[2]);
        newVI[oldVI[2] + 4] = totalnum;
        byte[] newV = SLSystem.intArrayToByteArray(newVI);

        for (int i = versionNum + 2;i < headInfoI.length;i ++){
            headInfoI[i] += 4;
        }
        byte[] headInfo = SLSystem.intArrayToByteArray(headInfoI);

        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(uri);
        ArrayList<byte[]> V = new ArrayList<>();
        V.add(headInfo);
        V.add(pre);
        V.add(newV);
        V.add(las);
        storeDataHDFS.storeData(V);

        return true;
    }

    /**
     *  删除特定版本数据库表中的某条记录
     * @param versionNum 版本号
     * @param num 记录编号
     * @param database 数据库名
     * @param table 表名
     * @return 成功或失败标志
     * @throws IOException
     * @throws DataFormatException
     */
    public boolean removeRecord(int versionNum, int num, String database,String table) throws IOException, DataFormatException {
        String uri = SLSystem.getURIVersion(database,table);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);
        int[] headInfoI = getHeadInfo(loadDataHDFS,uri,versionNum);

        int totalV = headInfoI[0];
        byte[] pre = getVersionBody(loadDataHDFS,0,versionNum);
        byte[] oldV = getVersionBody(loadDataHDFS,versionNum, versionNum + 1);
//        System.out.println(versionNum + " " + totalV);
        byte[] las = getVersionBody(loadDataHDFS,versionNum + 1,totalV);

        int[] oldVI = SLSystem.byteArrayToIntArray(oldV);
        oldVI[3] ++;
        int off = oldVI[2] + oldVI[3] + 4;

//        System.out.println("oldVI[2] = " + oldVI[2]);
        int[] newVI = new int[oldVI.length + 1];
//        System.out.println("newvI.length = " + newVI.length);
        System.arraycopy(oldVI,0,newVI,0,off);
        System.arraycopy(oldVI,off,newVI,off + 1,oldVI.length - off);
        newVI[off] = num;
        byte[] newV = SLSystem.intArrayToByteArray(newVI);

        for (int i = versionNum + 2;i < headInfoI.length;i ++){
            headInfoI[i] += 4;
        }
        byte[] headInfo = SLSystem.intArrayToByteArray(headInfoI);

        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(uri);
        ArrayList<byte[]> V = new ArrayList<>();
        V.add(headInfo);
        V.add(pre);
        V.add(newV);
        V.add(las);
        storeDataHDFS.storeData(V);
        return true;
    }

    /**
     *  替换特定版本数据库中的一条记录
     * @param versionNum 版本号
     * @param oldnum 记录原编号
     * @param newRecord 新记录编号
     * @param database 数据库名
     * @param table 表名
     * @return 成功或失败标志
     * @throws IOException
     * @throws DataFormatException
     */
    public boolean replaceRecord(int versionNum, int oldnum, String newRecord, String database, String table) throws IOException, DataFormatException {
        String uri = SLSystem.getURIVersion(database,table);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);
        int[] headInfoI = getHeadInfo(loadDataHDFS,uri,versionNum);

        StoreRecords storeRecords = new StoreRecords();
        int totalnum = storeRecords.appendRecords(newRecord,database,table);

        int totalV = headInfoI[0];
        byte[] pre = getVersionBody(loadDataHDFS,0,versionNum);
        byte[] oldV = getVersionBody(loadDataHDFS,versionNum, versionNum + 1);
//        System.out.println(versionNum + " " + totalV);
        byte[] las = getVersionBody(loadDataHDFS,versionNum + 1,totalV);

        int[] oldVI = SLSystem.byteArrayToIntArray(oldV);
        oldVI[4] ++;
        System.out.println("oldVI[4] = " + oldVI[4]);
        int[] newVI = new int[oldVI.length + 2];
//        System.out.println("newvI.length = " + newVI.length);
        System.arraycopy(oldVI,0,newVI,0,oldVI.length);
        newVI[oldVI.length] = totalnum;
        newVI[oldVI.length + 1] = oldnum;
        byte[] newV = SLSystem.intArrayToByteArray(newVI);

        for (int i = versionNum + 2;i < headInfoI.length;i ++){
            headInfoI[i] += 8;
        }
        byte[] headInfo = SLSystem.intArrayToByteArray(headInfoI);

        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(uri);
        ArrayList<byte[]> V = new ArrayList<>();
        V.add(headInfo);
        V.add(pre);
        V.add(newV);
        V.add(las);
        storeDataHDFS.storeData(V);
        return true;
    }

    /**
     *  合并两个版本的数据库表，并生成一个新的版本
     * @param V1 版本一，也是新版本的父版本
     * @param V2 版本二
     * @param database 数据库名
     * @param table 表名
     * @return 新版本号
     * @throws IOException
     * @throws DataFormatException
     */
    public int mergeVersion(int V1, int V2, String database, String table) throws IOException, DataFormatException {
        String uri = SLSystem.getURIVersion(database,table);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);
        int[] headInfoI = getHeadInfo(loadDataHDFS,uri,V1 > V2 ? V1 : V2);

        ArrayList<Integer> FV1 = getFaVerionInfo(V1,loadDataHDFS,headInfoI);
        ArrayList<Integer> FV2 = getFaVerionInfo(V2,loadDataHDFS,headInfoI);

        HashSet<Integer> recordNum1 = getAllRecordsNum(FV1,loadDataHDFS,headInfoI);
        HashSet<Integer> recordNum2 = getAllRecordsNum(FV2,loadDataHDFS,headInfoI);

        int comFa = getCommonFather(FV1,FV2);

        ArrayList<HashMap<Integer,Integer>> record1 = getRelativeChanges(FV1,comFa,loadDataHDFS,headInfoI);
        ArrayList<HashMap<Integer,Integer>> record2 = getRelativeChanges(FV2,comFa,loadDataHDFS,headInfoI);
        HashMap<Integer,Integer> added1 = record1.get(0);
        HashMap<Integer,Integer> added2 = record2.get(0);
        Set<Integer> addS2 = added2.keySet();

        addS2.removeAll(added1.keySet());

        HashMap<Integer,Integer> replaced1 = record1.get(2);
        HashMap<Integer,Integer> replaced2 = record2.get(2);

        cutReplaced(replaced1);
        cutReplaced(replaced2);

        mergeReplaced(replaced1,replaced2);

        ArrayList<Object> sum = new ArrayList<>();
        sum.add(addS2);
        sum.add(null);
        sum.add(replaced1);

        return createNewVersionV2(V1,sum,database,table);

    }

    private int createNewVersionV2(int v1, ArrayList<Object> sum, String database, String table) throws IOException, DataFormatException {
        String uri = SLSystem.getURIVersion(database,table);
        LoadDataHDFS loadDataHDFS = new LoadDataHDFS(uri);

        ArrayList<byte[]> newRecord = new ArrayList<>();

        byte[] num = new byte[4];
        loadDataHDFS.read(0,num,0,4);
        int VersionNum = SLSystem.byteArrayToInt(num,0);
        byte[] oldHead = new byte[VersionNum * 4 + 8];
        loadDataHDFS.read(0,oldHead,0,oldHead.length);

        byte[] tmp = getVersionBody(loadDataHDFS,0, VersionNum);

        Set<Integer> addedData = (Set<Integer>) sum.get(0);
        Set<Integer> removedData = (Set<Integer>) sum.get(1);
        HashMap<Integer,Integer> replacedData = (HashMap<Integer, Integer>) (sum.get(2));

        int newVersion = VersionNum;
        int addedNum = 0;
        int removedNum = 0;
        int replacedNum = 0;

        if (addedData != null && !addedData.isEmpty()){
            addedNum += addedData.size();
        }
        if (removedData != null && !removedData.isEmpty()){
            removedNum += removedData.size();
        }
        if (replacedData != null && !replacedData.isEmpty()){
            replacedNum += replacedData.size();
        }

        newRecord.add(SLSystem.intToByteArray(newVersion));
        newRecord.add(SLSystem.intToByteArray(v1));
        newRecord.add(SLSystem.intToByteArray(addedNum));
        newRecord.add(SLSystem.intToByteArray(removedNum));
        newRecord.add(SLSystem.intToByteArray(replacedNum));
        if (addedData != null){
            addedData.forEach(data -> {
                newRecord.add(SLSystem.intToByteArray(data));
            });
        }
        if (removedData != null){
            removedData.forEach(data -> {
                newRecord.add(SLSystem.intToByteArray(data));
            });
        }
        if (replacedData != null){
            for(Integer data : replacedData.keySet()){
                newRecord.add(SLSystem.intToByteArray(data));
                newRecord.add(SLSystem.intToByteArray(replacedData.get(data)));
            }
        }
        byte[] endL = new byte[4]; //存储版本记录的结束位置
        loadDataHDFS.read(VersionNum * 4 + 4,endL,0,4);
        int endLI = SLSystem.byteArrayToInt(endL,0);
        byte[] newL = SLSystem.intToByteArray(endLI + newRecord.size() * 4 + 4);

//        System.out.println("oldHead = " + SLSystem.byteArrayToInt(oldHead,0));
        oldHead = addN(oldHead,4);
//        System.out.println("oldHead = " + SLSystem.byteArrayToInt(oldHead,0));
        byte[] newHead = new byte[oldHead.length + 4];
        System.arraycopy(oldHead,0,newHead,0,oldHead.length);
        System.arraycopy(newL,0,newHead,oldHead.length,newL.length);

        ArrayList<byte[]> result = new ArrayList<>();
        result.add(newHead);
        result.add(tmp);
        result.addAll(newRecord);
        loadDataHDFS.destroy();

        StoreDataHDFS storeDataHDFS = new StoreDataHDFS(uri);
        storeDataHDFS.storeData(result);
        return newVersion;
    }

    private void mergeReplaced(HashMap<Integer, Integer> replaced1, HashMap<Integer, Integer> replaced2) {
        replaced1.putAll(replaced2);
    }

    private void cutReplaced(HashMap<Integer, Integer> replaced1) {
        Set<Integer> replaceS1 = replaced1.keySet();
        Integer[] list1 = replaceS1.toArray(new Integer[0]);
        Arrays.sort(list1);
        for (int i = list1.length - 1;i >= 0;i --){
            Integer tmp = replaced1.get(list1[i]);
            if (tmp == null) continue;

            Integer tmp1;
            while((tmp1 = replaced1.get(tmp)) != null){
                replaced1.remove(tmp);
                tmp = tmp1;
            }
            replaced1.replace(list1[i],tmp);
        }
    }

    private ArrayList<HashMap<Integer,Integer>> getRelativeChanges(ArrayList<Integer> vNums, int comFa, LoadDataHDFS loadDataHDFS, int[] headInfoI) throws IOException {
        ArrayList<HashMap<Integer,Integer>> recordNum = new ArrayList<>();
        HashMap<Integer,Integer> added = new HashMap<>();
        HashMap<Integer,Integer> removed = new HashMap<>();
        HashMap<Integer,Integer> replaced = new HashMap<>();
//        System.out.println("vNums.size() = " + vNums.size());

        for (int i = vNums.size() - 1;i >=0;i --){
            int vTmp = vNums.get(i);
            if (vTmp < comFa){
                continue;
            }
//            System.out.println("vTmp = " + vTmp);
            int dataS = headInfoI[vTmp + 1];
            int dataE = headInfoI[vTmp + 2];
//            System.out.println("dataS = " + dataS + ",dataE = " + dataE);
            byte[] datas = new byte[dataE - dataS];
            loadDataHDFS.read(dataS,datas,0,datas.length);
            ArrayList<HashMap<Integer,Integer>> changedInfo = getChangesV2(datas);
            added.putAll(changedInfo.get(0));
            removed.putAll(changedInfo.get(1));
            replaced.putAll(changedInfo.get(2));
        }
        recordNum.add(added);
        recordNum.add(removed);
        recordNum.add(replaced);
        return recordNum;
    }

    private ArrayList<HashMap<Integer,Integer>> getChangesV2(byte[] datas) {
        int[] datasI = SLSystem.byteArrayToIntArray(datas);
        int addedV = datasI[2];
        int removedV = datasI[3];
//        int changedV = datas[4];
//        System.out.println(addedV + " " + removedV + " " + changedV);
        HashMap<Integer,Integer> addedHash = new HashMap<>();
        HashMap<Integer,Integer> removedHash = new HashMap<>();
        HashMap<Integer,Integer> replacedHash = new HashMap<>();
        for (int i = 5;i < 5 + addedV;i ++){
            addedHash.put(datasI[i],datasI[i]);
        }
        for (int i = 5 + addedV;i < 5 + addedV + removedV;i ++){
            removedHash.put(datasI[i],datasI[i]);
        }
        for (int i = 5 + addedV + removedV;i < datasI.length;i += 2){
            replacedHash.put(datasI[i],datasI[i + 1]);
        }
        ArrayList<HashMap<Integer,Integer>> result = new ArrayList<>();
        result.add(addedHash);
        result.add(removedHash);
        result.add(replacedHash);
        return result;
    }

    private int getCommonFather(ArrayList<Integer> fv1, ArrayList<Integer> fv2) {
        int i = 0,j = 0;
        while (i < fv1.size() && j < fv2.size()){
            if (fv1.get(i) < fv2.get(j)){
                j ++;
            } else if (fv1.get(i) > fv2.get(j)){
                i ++;
            } else {
                break;
            }
        }
        return fv1.get(i);
    }
}
