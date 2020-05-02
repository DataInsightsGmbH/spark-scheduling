object SimpleJob extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val parquetWriter = new ParquetWriter
    parquetWriter.writeParquet("test2")
  }
}