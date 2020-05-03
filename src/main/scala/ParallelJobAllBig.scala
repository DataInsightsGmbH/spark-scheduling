object ParallelJobAllBig extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val listOfFiles = List.range(0, 5, 1).map(x => s"parallelTest$x")
    val parquetWriter = new ParquetWriterBig(args(0))
    for (fileName <- listOfFiles) {
      val thread = new Thread {
        override def run = {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
          parquetWriter.writeParquet(fileName)
        }
      }
      thread.start()
    }
  }
}
