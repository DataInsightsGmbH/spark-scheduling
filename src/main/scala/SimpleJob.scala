object SimpleJob extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val listOfFiles = List("parallelTest1", "parallelTest2", "parallelTest3")
    val parquetWriter = new ParquetWriter(args(0))
    for (file <- listOfFiles) {
      parquetWriter.writeParquet(file)
    }
    Thread.sleep(500000)
    println("Finished loading")
  }
}