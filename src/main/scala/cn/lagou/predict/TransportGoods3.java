package cn.lagou.predict;

import java.io.IOException;
import java.net.URISyntaxException;

public class TransportGoods3 {

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {

        String day = "20200608";
        String hdfsDir = "/replenishment";
        String hdfsResDir = "/res";

        String localDir = "/Users/yufangxing/Programme/Study/Spark/SparkDevelop/src/main/resources/replenishment";
        String localResDir = "/Users/yufangxing/Programme/Study/Spark/SparkDevelop/src/main/resources/res";

        Integer capacity = 40 * 1000;

        HdfsDownLoader.downloadFile(hdfsDir + "/" + day, localDir + "/" + day);

    }

}
