object ParallelJobOneBig extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val pathToFile = args(0)
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
    val parquetWriter = new ParquetWriter(pathToFile)
    val parquetWriterBig = new ParquetWriterBig(pathToFile)
    val threadBig = new Thread {
      override def run = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
        parquetWriterBig.writeParquet("bigFile")
      }
    }
    threadBig.start()

    val threadShort1 = new Thread {
      override def run = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
        parquetWriter.writeParquet("smallFile1")
      }
    }
    threadShort1.start()

    val threadShort2 = new Thread {
      override def run = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
        parquetWriter.writeParquet("smallFile2")
      }
    }
    threadShort2.start()

    val threadShort3 = new Thread {
      override def run = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
        parquetWriter.writeParquet("smallFile3")
      }
    }
    threadShort3.start()

    val threadShort4 = new Thread {
      override def run = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
        parquetWriter.writeParquet("smallFile4")
      }
    }
    threadShort4.start()
  }
}