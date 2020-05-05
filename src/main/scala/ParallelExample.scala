import org.apache.spark.sql.functions.col

object ParallelExample extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    new Thread(new Runnable() {
      override def run(): Unit = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
        val input1 = spark.sparkContext.parallelize(1 to 1000000000, 8)
        val evenNumbers = input1.filter(nr => {
          val isEven = nr % 2 == 0
          if (nr == 180000) {
            Thread.sleep(3000L)
          }
          isEven
        })
        evenNumbers.count()
      }
    }).start()
    new Thread(new Runnable() {
      override def run(): Unit = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
        val input2 = spark.sparkContext.parallelize(1000000000 to 2000000000, 8)
        val oddNumbers = input2.filter(nr => {
          val isOdd = nr % 1000000 != 0
          if (nr == 2500) {
            Thread.sleep(3000L)
          }
          isOdd
        })
        oddNumbers.count()

      }
    }).start()

    Thread.sleep(100000)
  }
}
