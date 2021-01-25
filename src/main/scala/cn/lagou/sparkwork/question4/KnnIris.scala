package cn.lagou.sparkwork.question4

import org.apache.spark.sql.{Encoders, SparkSession}

object KnnIris {

  case class Iris(Id: Long,
                  SepalLengthCm: Double,
                  SepalWidthCm: Double,
                  PetalLengthCm: Double,
                  PetalWidthCm: Double,
                  Species: String,
                  var Distance: Option[Double])

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")

    val irisFilePath = this.getClass.getClassLoader.getResource("data/iris.csv").toString

    import spark.implicits._
    val irisDs = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .schema(Encoders.product[Iris].schema)
      .csv(irisFilePath)
      .as[Iris]

    def distanceMeasure(source: Iris, target: Iris): Iris = {
      val distance1 = Math.pow(source.SepalWidthCm - target.SepalWidthCm, 2);
      val distance2 = Math.pow(source.SepalLengthCm - target.SepalLengthCm, 2);
      val distance3 = Math.pow(source.PetalWidthCm - target.PetalWidthCm, 2);
      val distance4 = Math.pow(source.PetalLengthCm - target.PetalLengthCm, 2);
      source.Distance = Option(math.sqrt(distance1 + distance2 + distance3 + distance4))
      source
    }

    val targetIris = Iris(100000, 6.6, 3.0, 4.4, 5.0, "", Option(0.0))

    val species = irisDs.rdd
      .map(sourceIris => distanceMeasure(sourceIris, targetIris))
      .sortBy(iris => iris.Distance)
      .first().Species

    println(species)

  }

}
