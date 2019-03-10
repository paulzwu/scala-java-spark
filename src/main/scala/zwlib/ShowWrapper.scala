package zwlib

import java.io.ByteArrayOutputStream

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Executes a roll up-style query against Apache logs.
  *
  * This is adapted from Apache Spark GitHub: https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/LogQuery.scala
  */
object ShowWrapper {

  def getString(ds: Dataset[Row]): String = {

    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      ds.show()
    }
    new String(outCapture.toByteArray)
  }

  def main(args: Array[String]) {
    //var sparkconf2 = new SparkConf().setAppName() sparkConf().setAppName("Log 1");
    //val sc=new sparkcontext()

    lazy val sparkConf = new SparkConf()
      .setAppName("Learn Spark")
      .setMaster("local[*]")
      .set("spark.cores.max", "2")

    lazy val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val df = spark.sql(" select 1 a")

    println(getString(df))
  }


}
