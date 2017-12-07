import preprocess.GenerateTestData;

import java.io.IOException;

public class GainTestData {
    public static void main(String... args) throws IOException {
        GenerateTestData generateTestData = new GenerateTestData();
        generateTestData.gainData(10000 * 10000);
    }
}
