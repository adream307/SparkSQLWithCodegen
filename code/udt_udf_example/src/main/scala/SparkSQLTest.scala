import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._


package org.apache.spark.sql.myfunctions {

  import org.apache.spark.sql.catalyst.expressions.Expression
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes
  import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback


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

  case class my_foo2(inputExpr: Seq[Expression]) extends Expression with ExpectsInputTypes with CodegenFallback {
    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = {
      val left = inputExpr(0).eval(input).asInstanceOf[ArrayData]
      val right = inputExpr(1).eval(input).asInstanceOf[ArrayData]
      if (left != null && right != null) {
        val result_x = left.getDouble(0) + right.getDouble(0)
        val result_y = left.getDouble(1) + right.getDouble(1)
        val result = new my_point(result_x, result_y)
        new my_point_udt().serialize(result)
      }
      else {
        null
      }
    }

    override def dataType: DataType = new my_point_udt

    override def inputTypes: Seq[AbstractDataType] = Seq(new my_point_udt, new my_point_udt)

    override def children: Seq[Expression] = inputExpr
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

    val my_foo1 = (x: my_point, y: my_point) => {
      if ((x == null) || (y == null)) null
      else new my_point(x.x + y.x, x.y + y.y)
    }
    spark.udf.register("my_foo1", my_foo1)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("my_foo2", my_foo2)

    val sch = StructType(Array(StructField("idx", IntegerType, false), StructField("point1", new my_point_udt, true), StructField("point2", new my_point_udt, true)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(raw_data), sch)
    df.createOrReplaceTempView("data_test")

    val test_sql = spark.sql("select idx, point1, point2, my_foo1(point1,point2)  from data_test")
    test_sql.explain()
    test_sql.show()

    val test_sql2 = spark.sql("select idx, point1, point2, my_foo2(point1,point2)  from data_test")
    test_sql2.explain()
    test_sql2.show()

    spark.stop()

  }

}