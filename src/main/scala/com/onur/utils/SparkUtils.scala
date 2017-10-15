package com.onur.utils

import com.onur.entity.CarDealer
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Onur_Dincol on 10/15/2017.
  */
object SparkUtils {

  def getSparkSession(appName: String) = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
      .getOrCreate()
    spark
  }

  def parseLog(x:Row) : Option[CarDealer] = {
    val fields = x.getString(0).split("\t")
    if (fields.length == 6) {
      return Some(CarDealer(
        fields(0),
        fields(1),
        fields(2),
        fields(3),
        fields(4),
        fields(5)
      ))
    } else {
      return None
    }
  }

  def loggingSettings() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}
