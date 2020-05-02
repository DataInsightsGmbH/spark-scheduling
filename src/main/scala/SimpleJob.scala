import org.apache.spark.sql.SparkSession

object SimpleJob extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val df = spark
      .read
      .option("header", true)
      .option("multiline", true)
      .csv("/marin/train.csv")

    df.write
      .mode("overwrite")
      .parquet("marin/train1")
  }
}