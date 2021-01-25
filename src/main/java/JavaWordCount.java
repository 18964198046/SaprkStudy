import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaWordCount {

    public static void main(String[] args) {
        // 1.创建SparkContext
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("JavaWordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");
        // 2.生成RDD
        JavaRDD<String> lines = jsc.textFile("/wcinput/wc.txt");
        // 3.RDD转换
        JavaRDD<String> words = lines.flatMap(line -> Arrays.stream(line.split("//s+")).iterator());
        JavaPairRDD<String, Integer> wordMap = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> result = wordMap.reduceByKey((x, y) -> x + y);
        // 4.结果输出
        result.foreach(elem -> System.out.println(elem));
        // 5.关闭SparkContext
        jsc.close();
    }

}
