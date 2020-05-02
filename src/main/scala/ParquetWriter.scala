import SimpleJob.spark

class ParquetWriter(path: String) extends SparkSessionWrapper {
  def writeParquet(fileName: String): Unit = {
    val df = spark
      .read
      .option("header", true)
      .option("multiline", true)
      .csv(s"$path/train.csv")

    df.write
      .mode("overwrite")
      .parquet(s"$path/$fileName")
    Thread.sleep(5000)
    df.show()
  }
}
