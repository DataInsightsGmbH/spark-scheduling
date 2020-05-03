import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col

class ParquetWriterBig(path: String) extends SparkSessionWrapper {
  val log: Logger = Logger.getLogger(getClass.getName)
  def writeParquet(fileName: String): Unit = {
    log.info(s"writing into the file $fileName, using sparkSession $spark")
    val df = spark
      .read
      .parquet(s"$path/train_big")
      .repartition(4)
      .where((col("id") % 2) === 0)

    df.write
      .mode("overwrite")
      .parquet(s"$path/$fileName")
    Thread.sleep(500000)
  }
}
