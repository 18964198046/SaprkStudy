package cn.lagou.sparkwork.question5

import org.apache.spark.sql.{Encoders, SparkSession}

import scala.Double.NaN
import scala.collection.mutable.ArrayBuffer

object KMeansIris {

  case class Iris(Id: Long,
                  SepalLengthCm: Double,
                  SepalWidthCm: Double,
                  PetalLengthCm: Double,
                  PetalWidthCm: Double,
                  Species: String) {
    var DistanceRed: Double = 0.0
    var DistanceBlue: Double = 0.0
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")

    val irisFilePath = this.getClass.getClassLoader.getResource("data/iris.csv").toString

    import spark.implicits._
    val irisDF = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .schema(Encoders.product[Iris].schema)
      .csv(irisFilePath)

    val irisDS = irisDF.as[Iris]

    val irisArray: Array[Iris] = irisDS.collect()

    val irises: Array[Iris] = irisDS.sample(false, 1).take(2)
    var red: Iris = irises(0)
    var blue: Iris = irises(1)
    var reds: ArrayBuffer[Iris] = ArrayBuffer[Iris]()
    var blues: ArrayBuffer[Iris] = ArrayBuffer[Iris]()

    var measure: Boolean = true

    while (measure) {
      irisArray.foreach(iris => {
        iris.DistanceRed = distance(iris, red)
        iris.DistanceBlue = distance(iris, blue)
//        println("========================")
//        println(distance(iris, red))
//        println(distance(iris, blue))
//        println(iris.DistanceRed)
//        println(iris.DistanceBlue)
      })
      reds = ArrayBuffer[Iris]()
      blues = ArrayBuffer[Iris]()
      irisArray.foreach(iris => {
        if (iris.DistanceRed > iris.DistanceBlue)
          reds += iris
        else
          blues += iris
      })
      val newRed = center(reds)
      val newBlue = center(blues)

      if (!change(red, newRed) && !change(blue, newBlue)) {
        measure = false
      }
      red = newRed
      blue = newBlue

    }

  }

  def distance(source: Iris, target: Iris): Double = {
    val distance1 = Math.pow(source.SepalWidthCm - target.SepalWidthCm, 2);
    val distance2 = Math.pow(source.SepalLengthCm - target.SepalLengthCm, 2);
    val distance3 = Math.pow(source.PetalWidthCm - target.PetalWidthCm, 2);
    val distance4 = Math.pow(source.PetalLengthCm - target.PetalLengthCm, 2);
    math.sqrt(distance1 + distance2 + distance3 + distance4)
  }

  def center(irises: ArrayBuffer[Iris]): Iris = {
    var sumSepalWidthCm: Double = 0.0
    var sumSepalLengthCm: Double = 0.0
    var sumPetalWidthCm: Double = 0.0
    var sumPetalLengthCm: Double = 0.0
    for (iris <- irises.iterator) {
      sumSepalWidthCm += iris.SepalWidthCm
      sumSepalLengthCm += iris.SepalLengthCm
      sumPetalWidthCm += iris.PetalWidthCm
      sumPetalLengthCm += iris.PetalLengthCm
    }
    sumSepalWidthCm /= irises.size
    sumSepalLengthCm /= irises.size
    sumPetalWidthCm /= irises.size
    sumPetalLengthCm /= irises.size
    Iris(0, sumSepalWidthCm, sumSepalLengthCm, sumPetalWidthCm, sumPetalLengthCm, "")
  }

  def change(oldPoint: Iris, newPoint: Iris): Boolean = {
    val distance1 = math.abs(oldPoint.SepalWidthCm - newPoint.SepalWidthCm)
    val distance2 = math.abs(oldPoint.SepalLengthCm - newPoint.SepalLengthCm)
    val distance3 = math.abs(oldPoint.PetalWidthCm - newPoint.PetalWidthCm)
    val distance4 = math.abs(oldPoint.PetalLengthCm - newPoint.PetalLengthCm)
    (distance1 + distance2 + distance3 + distance4) > 0.001
  }

}
