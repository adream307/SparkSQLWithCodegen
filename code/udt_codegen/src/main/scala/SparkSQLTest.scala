import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._


package org.apache.spark.sql.myfunctions {

  import org.apache.spark.sql.catalyst.expressions.Expression
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes
  import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}


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

  case class my_foo(inputExpr: Seq[Expression]) extends Expression with ExpectsInputTypes {
    override def nullable: Boolean = inputExpr(0).nullable || inputExpr(1).nullable

    override def eval(input: InternalRow): Any = ???

    override def dataType: DataType = new my_point_udt

    override def inputTypes: Seq[AbstractDataType] = Seq(new my_point_udt, new my_point_udt)

    override def children: Seq[Expression] = inputExpr

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      import org.apache.spark.sql.catalyst.expressions.codegen._
      import org.apache.spark.sql.catalyst.expressions.codegen.Block._

      val left_expr = inputExpr(0)
      val right_expr = inputExpr(1)
      val left_code = left_expr.genCode(ctx)
      val right_code = right_expr.genCode(ctx)

      if (nullable) {
        val nullSafeEval =
          left_code.code + ctx.nullSafeExec(left_expr.nullable, left_code.isNull) {
            right_code.code + ctx.nullSafeExec(right_expr.nullable, right_code.isNull) {
              s"""
                 |${ev.isNull} = false; // resultCode could change nullability.
                 |${CodeGenerator.javaType(DoubleType)} ${ev.value}_x = ${left_code.value}.getDouble(0) + ${right_code.value}.getDouble(0);
                 |${CodeGenerator.javaType(DoubleType)} ${ev.value}_y = ${left_code.value}.getDouble(1) + ${right_code.value}.getDouble(1);
                 |org.apache.spark.sql.myfunctions.my_point ${ev.value}_p = new org.apache.spark.sql.myfunctions.my_point(${ev.value}_x, ${ev.value}_y);
                 |org.apache.spark.sql.myfunctions.my_point_udt ${ev.value}_u = new org.apache.spark.sql.myfunctions.my_point_udt();
                 |${ev.value} = ${ev.value}_u.serialize(${ev.value}_p);
                 |""".stripMargin
            }
          }
        ev.copy(code =
          code"""
                boolean ${ev.isNull} = true;
                ${CodeGenerator.javaType(ArrayType(DoubleType, false))} ${ev.value} = null;
                $nullSafeEval
              """
        )
      } else {
        ev.copy(code =
          code"""
                ${left_code.code}
                ${right_code.code}
                ${CodeGenerator.javaType(DoubleType)} ${ev.value}_x = ${left_code.value}.getDouble(0) + ${right_code.value}.getDouble(0);
                ${CodeGenerator.javaType(DoubleType)} ${ev.value}_y = ${left_code.value}.getDouble(1) + ${right_code.value}.getDouble(1);
                org.apache.spark.sql.myfunctions.my_point ${ev.value}_p = new org.apache.spark.sql.myfunctions.my_point(${ev.value}_x, ${ev.value}_y);
                org.apache.spark.sql.myfunctions.my_point_udt ${ev.value}_u = new org.apache.spark.sql.myfunctions.my_point_udt();
                ${CodeGenerator.javaType(ArrayType(DoubleType, false))} ${ev.value} = ${ev.value}_u.serialize(${ev.value}_p);
              """, FalseLiteral)
      }
    }
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
      Row(3, null, new my_point(30, 30)),
      Row(4, new my_point(4, 4), null),
      Row(5, null, null)
    )

    spark.sessionState.functionRegistry.createOrReplaceTempFunction("my_foo", my_foo)

    val sch = StructType(Array(StructField("idx", IntegerType, false), StructField("point1", new my_point_udt, true), StructField("point2", new my_point_udt, true)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(raw_data), sch)
    df.createOrReplaceTempView("data_test")

    val test_sql = spark.sql("select idx, point1, point2, my_foo(point1,point2)  from data_test")
    test_sql.explain()
    test_sql.queryExecution.debug.codegen()
    test_sql.show()

    println("=========================")

    val raw_data2 = Seq(
      Row(1, new my_point(1, 1), new my_point(10, 10)),
      Row(2, new my_point(2, 2), new my_point(20, 20)),
      Row(3, new my_point(3, 3), new my_point(30, 30)),
      Row(4, new my_point(4, 4), new my_point(40, 40)),
      Row(5, new my_point(5, 5), new my_point(50, 50))
    )
    val sch2 = StructType(Array(StructField("idx", IntegerType, false), StructField("point1", new my_point_udt, false), StructField("point2", new my_point_udt, false)))
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(raw_data2), sch2)
    df2.createOrReplaceTempView("data_test2")

    val test_sql2 = spark.sql("select idx, point1, point2, my_foo(point1,point2)  from data_test2")
    test_sql2.explain()
    test_sql2.queryExecution.debug.codegen()
    test_sql2.show()

    spark.stop()

  }

}