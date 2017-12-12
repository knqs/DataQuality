package preprocess;

import java.io.*;
import java.util.Random;

public class GenerateTestData {
    String filename = "/home/kongning/文档/数据质量/testdata.txt";
    String[] attrName = null;
    String[] attrType = null;
    int[] attrTypeCode = null;
    int recordNum = 1000 * 10000;

    public GenerateTestData(int intnum, int doublenum, int stringnum){
        int totalnum = intnum + doublenum + stringnum;
        attrName = new String[totalnum];
        attrType = new String[totalnum];
        attrTypeCode = new int[totalnum];
        for (int i = 0;i < intnum;i ++){
            attrName[i] = "int" + i;
            attrType[i] = "int";
            attrTypeCode[i] = 0;
        }
        for (int i = intnum;i < intnum + doublenum;i ++){
            attrName[i] = "double" + i;
            attrType[i] = "double";
            attrTypeCode[i] = 1;
        }
        for (int i = intnum + doublenum;i < totalnum;i ++){
            attrName[i] = "string" + i;
            attrType[i] = "string";
            attrTypeCode[i] = 2;
        }
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
