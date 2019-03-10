
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package sample

import java.io.ByteArrayOutputStream

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//import com.example.hello._

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
