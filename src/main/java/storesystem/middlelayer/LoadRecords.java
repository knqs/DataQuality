package storesystem.middlelayer;

import Utils.SLSystem;
import storesystem.underlying.LoadDataHDFS;
import storesystem.underlying.LoadDataInterface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LoadRecords implements LoadRecordsInterface {

    String dataBaseName;
    String tableName;
    LoadDataInterface loadData;

    ArrayList<String> attrAndType = new ArrayList<String>();

    public LoadRecords(String dataBaseName, String tableName) throws IOException {
        this.dataBaseName = dataBaseName;
        this.tableName = tableName;
        Init();
    }

    private void Init() throws IOException {
        String uri = SLSystem.getURI(dataBaseName, tableName);
        loadData = new LoadDataHDFS(uri);
        checkDatabase();
        getAttributes();
    }

    private void getAttributes() throws IOException {
        String str;
        String[] strs;
        while(true){
            str = loadData.readLine();
            strs = str.split(" ");
            if(strs[0].toLowerCase().equals("@data")){
                break ;
            }
            else if(!strs[0].toLowerCase().equals("@attributes")){
                System.err.print("DataError in Database " + dataBaseName + " and table " + tableName);
                break;
            }
            else{
                attrAndType.add(strs[1]);
                attrAndType.add(strs[2]);
            }
        }
    }

    /**
     * 检查数据库是否一致
     */
    private void checkDatabase() throws IOException {
        String[] strs1, strs2;
        String str1, str2;
        if ((str1 = loadData.readLine()) == null){
            System.err.print("Database Name Error!");
        }
        if ((str2 = loadData.readLine()) == null){
            System.err.print("Table Name Error!");
        }
        strs1 = str1.split(" "); //分隔符依据指定的数据格式
        strs2 = str2.split(" ");
        if(strs1[1].equals(dataBaseName) && strs2[1].equals(tableName)){
        } else {
            System.err.print("Error! databaseName or tableName inconsistency!");
        }
    }

    @Override
    public String getRecord() throws IOException {
        return loadData.readLine();
    }

    @Override
    public String getRecords(int num) throws IOException {
        String result = "";
        String str;
        for(int i = 0;i < num;i ++){
            if((str = loadData.readLine()) != null) {
                result += str + "\n";
            } else {
                break;
            }
        }

        if(result.equals("")) return null;
        else return result;
    }

    @Override
    public List<String> getAttrsAndType() {
        return attrAndType;
    }
}
