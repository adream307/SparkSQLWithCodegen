import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkSQLTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark sql test")
      .getOrCreate()

    val raw_data = Seq(
      Row(1.0, 2.0),
      Row(3.0, 4.0),
      Row(5.0, null),
      Row(null, 6.0),
      Row(null, null)
    )

    val sch = StructType(Array(StructField("x", DoubleType, true), StructField("y", DoubleType, true)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(raw_data), sch)
    df.createOrReplaceTempView("data_test")

    val test_sql = spark.sql("select x, y, power(x,y) from data_test")
    test_sql.queryExecution.debug.codegen()
    test_sql.show()
    spark.stop()

  }

}