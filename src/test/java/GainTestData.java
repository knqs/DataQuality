import preprocess.GenerateTestData;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GainTestData {
    public static void main(String... args) throws IOException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        System.out.println(simpleDateFormat.format(new Date()));
        GenerateTestData generateTestData = new GenerateTestData(20, 20, 20);
        generateTestData.gainData(10000 * 15000);
        System.out.println(simpleDateFormat.format(new Date()));
    }
}
