package com.onur.stream

import com.onur.utils.SparkUtils._

/**
  * Created by Onur_Dincol on 10/15/2017.
  */
object CarDealerStructuredStreaming {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession("CarDealerStructuredStreaming")
    loggingSettings()
    val rawData = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._

    val carMakerSells = rawData.flatMap(parseLog).select("carMaker").groupBy("carMaker").count().orderBy($"count".desc)
    val query = carMakerSells.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
    spark.stop()
  }
}
