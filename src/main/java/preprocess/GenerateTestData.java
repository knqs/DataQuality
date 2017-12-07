package preprocess;

import java.io.*;
import java.util.Random;

public class GenerateTestData {
    String filename = "/home/kongning/文档/数据质量/testdata.txt";
    String[] attrName = new String[]{"id_type", "freq", "value", "name", "id", "time"};
    String[] attrType = new String[]{"int", "int", "string", "string", "string", "int"};
    int[] attrTypeCode = new int[]{0,0,2,2,2,0};
    int recordNum = 1000 * 10000;

    public GenerateTestData(){

    }

    public void gainData(String filename) throws IOException {
        this.filename = filename;
        gainData();
    }

    public void gainData(int length) throws IOException {
        this.recordNum = length;
        gainData();
    }

    public void gainData() throws IOException {
        BufferedWriter bfw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "utf-8"));
        bfw.write(gainHead());
        for(int i = 0;i < recordNum;i ++){
            bfw.write(gainRecord());
        }
        bfw.close();
    }

    private String gainHead() {
        StringBuffer result = new StringBuffer();
        result.append("@database KNQ\n" +
                "@table KNQTable\n");
        for (int i = 0;i < attrName.length;i ++){
            result.append("@attributes " + attrName[i] + " " + attrType[i] + "\n");
        }
        result.append("@data\n");
        return new String(result);
    }

    private String gainRecord() {
        StringBuffer result = new StringBuffer();
        Random random = new Random();
        for (int i = 0;i < attrTypeCode.length - 1;i ++){
            gainItem(result, i, random, " ");
        }
        gainItem(result, attrTypeCode.length - 1, random, "\n");
        return new String(result);
    }

    private void gainItem(StringBuffer result, int i, Random random, String end) {
        switch (attrTypeCode[i]){
            case 0:
                result.append(random.nextInt() + end);
                break;
            case 1:
                result.append(random.nextDouble() + end);
                break;
            case 2:
                result.append(gainString(Math.abs(random.nextInt()) % 20) + end);
                break;
            default:
                break;
        }
    }

    String[] charSet = new String[]{"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
    private String gainString(int length) {
        StringBuffer result = new StringBuffer();
        Random random = new Random();
        for (int i = 0;i < length;i ++){
            result.append(charSet[Math.abs(random.nextInt() % 26)]);
        }
        return new String(result);
    }


}
