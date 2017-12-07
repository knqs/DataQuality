package preprocess;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class formatChange {
    public static void main(String... args) throws IOException {
        formatChange formatChange = new formatChange();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        for (int i = 0;i < 20;i ++){
            System.out.println(i);
            System.out.println(simpleDateFormat.format(new Date()));
            formatChange.deal("/home/kongning/文档/数据质量/data/user_profile_sample_dealt.tsv." + i,"/home/kongning/文档/数据质量/dataaffr/user_profile_sample_dealt.affr." + i);
        }
    }

    String headInfo = "@database knqknq\n" +
            "@table knq\n" +
            "@attributes id_type int\n" +
            "@attributes freq int\n" +
            "@attributes value string\n" +
            "@attributes name string\n" +
            "@attributes id string\n" +
            "@attributes time int\n" +
            "@data\n";
    private void deal(String src, String dst) throws IOException {
        BufferedReader bfr = new BufferedReader(new FileReader(new File(src)));
        BufferedWriter bfw = new BufferedWriter(new FileWriter(new File(dst)));

        bfw.write(headInfo);
        String str;
        while ((str = bfr.readLine()) != null){
            String[] strs = str.split("\t");
            if (strs.length > 6) continue;
            StringBuffer stringBuffer = new StringBuffer();
            boolean flag = false;
            for (int i = 0;i < strs.length - 1;i ++){
                String[] tmp = strs[i].split(":");
                if (tmp.length < 2){
                    flag = true;
                    break;
                }
                stringBuffer.append(tmp[1].replace(" ","") + " ");
            }
            String[] tmp = strs[strs.length - 1].split(":");
            if (tmp.length < 2 || flag){
                continue;
            }
            stringBuffer.append( tmp[1]+ "\n");
            bfw.write(new String(stringBuffer));
        }
        bfr.close();
        bfw.close();
    }
}
