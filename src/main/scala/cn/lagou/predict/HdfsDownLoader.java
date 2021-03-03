package cn.lagou.predict;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsDownLoader {

    private static FileSystem fs;

//    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
//        //downloadFile("/wc.txt", "/Users/yufangxing/Programme/Study/Spark/SparkDevelop/src/main/resources/wc.txt");
//        downloadFolder("/test/output", "/Users/yufangxing/Programme/Study/Spark/SparkDevelop/src/main/resources/hdfs/");
//    }

    private static void init() throws URISyntaxException, IOException, InterruptedException {
        final Configuration conf  = new Configuration();
        fs = FileSystem.get(new URI("hdfs://lgns"), conf, "root");
        System.out.println(fs);
    }

    private static void destory() throws IOException {
        fs.close();
    }

    private static void testMkdir() throws IOException {
        fs.mkdirs(new Path("/cdh_test"));
    }

    public static void downloadFile( String srcPath, String dstPath) throws IOException, URISyntaxException, InterruptedException {
        init();
        try (FSDataInputStream in = fs.open(new Path(srcPath));
            FileOutputStream out = new FileOutputStream(dstPath)) {
            IOUtils.copyBytes(in, out, 4096);
        }
        destory();
    }

    public static void downloadFolder(String srcPath, String dstPath) throws IOException, URISyntaxException, InterruptedException {
        init();
        final File file = new File(dstPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        final FileStatus[] fileStatuses = fs.listStatus(new Path(srcPath));
        final Path[] paths = FileUtil.stat2Paths(fileStatuses);
        for (Path path : paths) {
            final String hdfsFilePath = path.toString();
            final int i = hdfsFilePath.lastIndexOf("/");
            final String hdfsFileName = hdfsFilePath.substring(i + 1);
            downloadFile(srcPath,dstPath + '/' + hdfsFileName);
        }
        destory();
    }

}
