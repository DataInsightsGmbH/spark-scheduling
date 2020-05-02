object ParallelJob extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val listOfFiles = List("parallelTest1", "parallelTest2", "parallelTest3")
    val parquetWriter = new ParquetWriter(args(0))
    for (fileName <- listOfFiles){
      val thread = new Thread{
        override def run: Unit = {
          parquetWriter.writeParquet(fileName)
        }
      }
      thread.start()
    }
  }
}
