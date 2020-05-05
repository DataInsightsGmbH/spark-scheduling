import java.util.concurrent.CountDownLatch

object ParallelJob extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val listOfFiles = List.range(0, 5, 1).map(x => s"parallelTest$x")
    val parquetWriter = new ParquetWriter(args(0))
    val latch = new CountDownLatch(1)
    for (fileName <- listOfFiles) {
      val thread = new Thread {
        override def run = {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
          parquetWriter.writeParquet(fileName)
        }
      }
      thread.start()
    }
    latch.await()
  }
}
