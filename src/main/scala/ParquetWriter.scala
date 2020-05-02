import SimpleJob.spark

class ParquetWriter extends SparkSessionWrapper {
  def writeParquet(fileName: String): Unit = {
    val df = spark
      .read
      .option("header", true)
      .option("multiline", true)
      .csv("/marin/train.csv")

    df.write
      .mode("overwrite")
      .parquet(s"marin/$fileName")
  }
}
