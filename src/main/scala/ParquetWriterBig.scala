import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col

class ParquetWriterBig(path: String) extends SparkSessionWrapper {
  val log: Logger = Logger.getLogger(getClass.getName)
  def writeParquet(fileName: String): Unit = {
    log.info(s"writing into the file $fileName")
    val df = spark
      .read
      .option("header", true)
      .option("multiline", true)
      .csv(s"$path/train_big.csv")
      .repartition(4)
      .where((col("id") % 2) === 0)

    df.write
      .mode("overwrite")
      .parquet(s"$path/$fileName")
    Thread.sleep(500000)
  }
}
