package com.onur.stream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Onur_Dincol on 10/14/2017.
  */

object CarDealerStreaming {

  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[*]", "CarDealerStreaming", Seconds(1))
    val cardealerLogs = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val carMakers = cardealerLogs.map { logline => {
          logline.split("\t")(1)}
    }
    val carMakerSells = carMakers.map(carMaker => (carMaker, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(60), Seconds(10))
    val sortedcarMakerSells = carMakerSells.transform(rdd => rdd.sortBy(x => x._2, false))

      sortedcarMakerSells.print(5)
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
