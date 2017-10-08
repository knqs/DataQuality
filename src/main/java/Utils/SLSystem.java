package Utils;

public class SLSystem {
    public static String getURI(String databaseName, String tableName){
        return "hdfs://localhost:9000/database_" + databaseName + '/' + tableName;
    }
}
