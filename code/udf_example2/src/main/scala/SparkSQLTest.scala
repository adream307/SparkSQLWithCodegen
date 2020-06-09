import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

package org.apache.spark.sql.myfunctions {

  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
  import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
  import org.apache.spark.sql.types.{AbstractDataType, DoubleType}

  case class my_foo(inputExpressions: Seq[Expression]) extends Expression with ExpectsInputTypes with CodegenFallback {

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = {
      val left = inputExpressions(0).eval(input)
      val right = inputExpressions(1).eval(input)
      if (left != null && right != null) {
        left.asInstanceOf[Double] * right.asInstanceOf[Double] + 3
      }
      else {
        null
      }
    }

    override def dataType: DataType = DoubleType

    override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

    override def children: Seq[Expression] = inputExpressions
  }

}

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

    import org.apache.spark.sql.myfunctions.my_foo

    spark.sessionState.functionRegistry.createOrReplaceTempFunction("my_foo", my_foo)

    val test_sql = spark.sql("select x, y, my_foo(x,y) from data_test")
    test_sql.explain()
    test_sql.show()

    spark.stop()

  }

}