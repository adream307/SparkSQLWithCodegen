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
    override def nullable: Boolean = inputExpressions(0).nullable || inputExpressions(1).nullable

    override def eval(input: InternalRow): Any = ???

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val left_expr = inputExpressions(0)
      val right_expr = inputExpressions(1)
      val left_code = left_expr.genCode(ctx)
      val right_code = right_expr.genCode(ctx)

      if (nullable) {
        val nullSafeEval =
          left_code.code + ctx.nullSafeExec(left_expr.nullable, left_code.isNull) {
            right_code.code + ctx.nullSafeExec(right_expr.nullable, right_code.isNull) {
              s"""
                 | ${ev.isNull} = false; // resultCode could change nullability.
                 | ${ev.value} = ${left_code.value} * ${right_code.value};
                 | ${ev.value} = ${ev.value} + 3;
                 |""".stripMargin
            }
          }
        ev.copy(code =
          code"""
             boolean ${ev.isNull} = true;
             ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
             $nullSafeEval
              """)
      } else {
        ev.copy(code =
          code"""
             ${left_code.code}
             ${right_code.code}
             ${CodeGenerator.javaType(dataType)} ${ev.value} = ${left_code.value} * ${right_code.value};
             ${ev.value} = ${ev.value} + 3;
              """, FalseLiteral)
      }
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
    test_sql.queryExecution.debug.codegen()
    test_sql.show()

    println("=====================")

    val raw_data2 = Seq(
      Row(1.0, 2.0),
      Row(3.0, 4.0),
      Row(5.0, 6.0),
      Row(7.0, 8.0),
      Row(9.0, 10.0)
    )

    val sch2 = StructType(Array(StructField("x", DoubleType, false), StructField("y", DoubleType, false)))

    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(raw_data2), sch2)
    df2.createOrReplaceTempView("data_test2")

    val test_sql2 = spark.sql("select x, y, my_foo(x,y) from data_test2")
    test_sql2.explain()
    test_sql2.queryExecution.debug.codegen()
    test_sql2.show()

    spark.stop()

  }

}