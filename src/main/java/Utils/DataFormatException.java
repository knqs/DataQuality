package Utils;

public class DataFormatException extends Exception {
    public DataFormatException(String... info){
        System.err.print(info);
    }
}
