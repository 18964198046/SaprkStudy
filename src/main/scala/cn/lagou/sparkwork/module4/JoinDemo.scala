package cn.lagou.sparkwork.module4

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 关于join 操作何时是宽依赖，何时是窄依赖:
 *
 * 执行结果: rdd3的Join创建了宽依赖，rdd4的Join创建了窄依赖
 *
 * rdd3的Join是宽依赖的原因:
 * (1) 当执行Join操作时，如果用户不提供分区器，系统会默认使用defaultPartitioner方法来创建一个默认的分区器
 * (2) 在创建默认分区器时会需要获取默认的分区数，该数值会依赖spark.default.parallelism参数的配置，
 * 如果未设置spark.default.parallelism，则为根据SchedulerBackend中的defaultParallelism方法来决定分区数量。
 * 由于Spark已本地模式运行，所以会使用LocalSchedulerBackend中的defaultParallelism方法: ("spark.default.parallelism", totalCores)
 * 由于我的电脑是6核CPU，虚拟12个核心，因此Spark获取到的totalCores=12，该值会决定Spark在makeRDD时numSlices的数据切片的数量，也就是分多少个区
 * (3) 又由于hasMaxPartitioner，系统返回了new HashPartitioner(defaultNumPartitions)，作为默认的分区器
 * (4) Spark通过rdd.partitioner == Some(part)来判断，由于rdd.partitioner为None，所以判断结果为false
 * (5) 最终创建了宽依赖ShuffleDependency
 *
 * rdd4的Join是窄依赖的原因:
 *
 * (1) 由于原因相似，但在创建分区器时，由于isEligiblePartitioner判断为true，因此直接返回了用户提供的HashPartitioner作为分区器
 * (2) Spark通过rdd.partitioner == Some(part)判断结果为true，因为HashPartitioner判断是否相等，只判断numPartitions是否相等，所以结果为true
 * (3) 最终创建了窄依赖OneToOneDependency
 *
 * 总结:
 * rdd.partitioner == Some(part)用于判断创建的依赖是宽依赖还是窄依赖，如果判断结果为true，创建窄依赖，否则创建宽依赖。
 * 如果等式两边不为空，看partitioner是否是HashPartitioner, 如果是HashPartitioner判断numPartitions是否相等。
 * 如果不是HashPartitioner，则看Partitioner是否是同一个对象
 */
object JoinDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val random = scala.util.Random
    val col1 = Range(1, 50).map(idx => (random.nextInt(10), s"user$idx"))
    val col2 = Array((0, "BJ"), (1, "SH"), (2, "GZ"), (3, "SZ"), (4, "TJ"), (5, "CQ"), (6, "HZ"), (7, "NJ"), (8, "WH"), (0, "CD"))
    val rdd1: RDD[(Int, String)] = sc.makeRDD(col1)
    val rdd2: RDD[(Int, String)] = sc.makeRDD(col2)
    val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
    val rdd4: RDD[(Int, (String, String))] = rdd1.partitionBy(new HashPartitioner(3))
      .join(rdd2.partitionBy(new HashPartitioner(3)))

    /**
     * (0,(CompactBuffer(user6, user7, user8, user12, user16, user29, user33, user41, user48),CompactBuffer(BJ, CD)))
     * (1,(CompactBuffer(user15, user18, user46),CompactBuffer(SH)))
     * (2,(CompactBuffer(user1, user2, user24, user25, user26),CompactBuffer(GZ)))
     * (3,(CompactBuffer(user3, user10, user17, user20, user35, user39, user44),CompactBuffer(SZ)))
     * (4,(CompactBuffer(user13, user14, user27, user43),CompactBuffer(TJ)))
     * (5,(CompactBuffer(user21, user38, user49),CompactBuffer(CQ)))
     * (6,(CompactBuffer(user4, user30, user34, user37, user42),CompactBuffer(HZ)))
     * (7,(CompactBuffer(user5, user9, user23, user40, user47),CompactBuffer(NJ)))
     * (8,(CompactBuffer(user22, user28, user36),CompactBuffer(WH)))
     * (9,(CompactBuffer(user11, user19, user31, user32, user45),CompactBuffer()))
     */
     rdd3.dependencies.foreach(d => d.rdd.collect.foreach(println))
     println("-------------------------------")

    /**
     * (0,(CompactBuffer(user6, user7, user8, user12, user16, user29, user33, user41, user48),CompactBuffer(BJ, CD)))
     * (6,(CompactBuffer(user4, user30, user34, user37, user42),CompactBuffer(HZ)))
     * (3,(CompactBuffer(user3, user10, user17, user20, user35, user39, user44),CompactBuffer(SZ)))
     * (9,(CompactBuffer(user11, user19, user31, user32, user45),CompactBuffer()))
     * (4,(CompactBuffer(user13, user14, user27, user43),CompactBuffer(TJ)))
     * (1,(CompactBuffer(user15, user18, user46),CompactBuffer(SH)))
     * (7,(CompactBuffer(user5, user9, user23, user40, user47),CompactBuffer(NJ)))
     * (8,(CompactBuffer(user22, user28, user36),CompactBuffer(WH)))
     * (5,(CompactBuffer(user21, user38, user49),CompactBuffer(CQ)))
     * (2,(CompactBuffer(user1, user2, user24, user25, user26),CompactBuffer(GZ)))
     */
    rdd4.dependencies.foreach(d => d.rdd.collect.foreach(println))
    println("-------------------------------")

    /**
     * 打印结果
     * org.apache.spark.OneToOneDependency@3e48e859
     * org.apache.spark.OneToOneDependency@1a5f7e7c
     */
    println(rdd3.dependencies)
    println(rdd4.dependencies)

    Thread.sleep(100000000)

    sc.stop()

  }

}
