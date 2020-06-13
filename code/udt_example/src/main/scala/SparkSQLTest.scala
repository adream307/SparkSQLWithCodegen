import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._


package org.apache.spark.sql.myfunctions {

  @SQLUserDefinedType(udt = classOf[my_point_udt])
  class my_point(val x: Double, val y: Double) extends Serializable {
    override def hashCode(): Int = 31 * (31 * x.hashCode()) + y.hashCode()

    override def equals(other: Any): Boolean = other match {
      case that: my_point => this.x == that.x && this.y == that.y
      case _ => false
    }

    override def toString(): String = s"($x, $y)"
  }

  class my_point_udt extends UserDefinedType[my_point] {
    override def sqlType: DataType = ArrayType(DoubleType, false)

    override def serialize(obj: my_point): GenericArrayData = {
      val output = new Array[Double](2)
      output(0) = obj.x
      output(1) = obj.y
      new GenericArrayData(output)
    }

    override def deserialize(datum: Any): my_point = {
      datum match {
        case values: ArrayData => new my_point(values.getDouble(0), values.getDouble(1))
      }
    }

    override def userClass: Class[my_point] = classOf[my_point]
  }

}

object SparkSQLTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark sql test")
      .getOrCreate()

    import org.apache.spark.sql.myfunctions._

    val raw_data = Seq(
      Row(1, new my_point(1, 1), new my_point(10, 10)),
      Row(2, new my_point(2, 2), new my_point(20, 20)),
      Row(3, new my_point(3, 3), new my_point(30, 30)),
      Row(4, new my_point(4, 4), new my_point(40, 40)),
      Row(5, new my_point(5, 5), new my_point(50, 50))
    )

    val sch = StructType(Array(StructField("idx", IntegerType, false), StructField("point1", new my_point_udt, false), StructField("point2", new my_point_udt, false)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(raw_data), sch)
    df.createOrReplaceTempView("data_test")


    val test_sql = spark.sql("select idx, point1, point2  from data_test")
    test_sql.explain()
    test_sql.show()

    spark.stop()

  }

}