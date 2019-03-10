package sample

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.types.{StructField, _}

import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

class ChronicPattern extends UserDefinedAggregateFunction {
  var pn_taret: Double = 0

  // This is the input fields for your aggregate function.
  override def inputSchema: StructType = StructType(
    StructField("OrderByField", StringType) ::
      //StructField("OrderByField", StringType) ::
      StructField("rate", DoubleType) :: Nil

  )

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("buff1", DataTypes.createArrayType(DataTypes.StringType, true)) ::
      StructField("buff2", DataTypes.createArrayType(DataTypes.DoubleType, true)) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StructType(
    StructField("ordered_field", StringType, true) ::
      StructField("rates", StringType, true) ::
      /*StructField("chronic", BooleanType,true)::
      StructField("firstBadDay", StringType,true)::
      StructField("lastBadDay", StringType,true)::
      StructField("firstExtraDay", StringType,true)::
      StructField("badDaySpread", IntegerType,true)::
      StructField("nbrBadDay", StringType,true)::*/ Nil
  )

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ArrayBuffer[String]()
    buffer(1) = ArrayBuffer[Double]()
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //var  orderByList:List[String] = buffer.get(0)
    buffer.update(0, buffer.getAs[ArrayBuffer[String]](0) :+ input.getString(0))
    //List[Double] numList = List(buffer.get(1))
    buffer.update(1, buffer.getAs[ArrayBuffer[Double]](1) :+ input.getDouble(1))

  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val v1 = buffer2.getList[String](0)
    //println(v)
    v1.foreach(v => {
      val b = buffer1.getAs[ArrayBuffer[String]](0) :+ v;
      buffer1.update(0, b)
      //println(b)
    })

    val v2 = buffer2.getList[Double](1)
    //println(v2)
    v2.foreach(v0 => {
      val b2 = buffer1.getAs[ArrayBuffer[Double]](1) :+ v0;
      buffer1.update(1, b2)
      //println("..." + v0)
    })
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Row = {
    val v1 = buffer.getList[String](0)
    val v2 = buffer.getList[Double](1)
    var tm: TreeMap[String, Double] = new TreeMap;
    for (i <- 0 until v1.length) {
      tm += (v1.get(i) -> v2.get(i))
    }
    println(tm)
    //v2.foreach(v=> println(v))
    RowFactory.create("ordered", "values")
  }

}




