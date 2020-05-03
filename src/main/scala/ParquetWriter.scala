import SimpleJob.spark
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

class ParquetWriter(path: String) extends SparkSessionWrapper {
  val log: Logger = Logger.getLogger(getClass.getName)
  def writeParquet(fileName: String): Unit = {
    Thread.sleep(2000)
    log.info(s"writing into the file $fileName, using sparkSession $spark")
    val df = spark
      .read
      .option("header", true)
      .option("multiline", true)
      .csv(s"$path/train.csv")
      .repartition(4)
      .where((col("id") % 10) === 0)

    df.write
      .mode("overwrite")
      .parquet(s"$path/$fileName")
    Thread.sleep(500000)
  }
}
