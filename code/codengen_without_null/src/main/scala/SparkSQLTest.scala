import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

package org.apache.spark.sql.myfunctions {

  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions._
  import org.apache.spark.sql.catalyst.expressions.codegen._
  import org.apache.spark.sql.catalyst.expressions.codegen.Block._

  case class my_foo(inputExpressions: Seq[Expression]) extends Expression with ExpectsInputTypes {
    override def nullable: Boolean = false

    override def eval(input: InternalRow): Any = ???

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val left_code = inputExpressions(0).genCode(ctx)
      val right_code = inputExpressions(1).genCode(ctx)
      ev.copy(code =
        code"""
              | ${left_code.code}
              | ${right_code.code}
              |
              | ${CodeGenerator.javaType(DoubleType)} ${ev.value} = ${left_code.value} * ${right_code.value};
              | ${ev.value} = ${ev.value} + 3;
              |""".stripMargin, FalseLiteral)
    }

    override def dataType: DataType = DoubleType

    override def children: Seq[Expression] = inputExpressions

    override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)
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
      Row(3.0, 4.0)
    )

    val sch = StructType(Array(StructField("x", DoubleType, false), StructField("y", DoubleType, false)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(raw_data), sch)
    df.createOrReplaceTempView("data_test")

    import org.apache.spark.sql.myfunctions.my_foo

    spark.sessionState.functionRegistry.createOrReplaceTempFunction("my_foo", my_foo)

    val test_sql = spark.sql("select x, y, my_foo(x,y) from data_test")
    test_sql.explain()
    test_sql.queryExecution.debug.codegen()
    test_sql.show()

    spark.stop()

  }

}