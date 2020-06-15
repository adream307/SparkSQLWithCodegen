# 自定义数据类型
在之前的文章中，我们介绍了如何使用 `Codegen` 实现自定义函数，但是自定义函数参数类型及返回值类型均为 `Spark` 原生的数据类型。

从本篇文章开始，我们介绍如何在 `Spark` 中自定义数据类型 (UDT) ，以及针对该 `UDT` 的自定义函数，最后，我们希望这些自定义函数也是 Codegen 实现的

## UDT
自定义数据类型的要求如下：
1. `UDT` 的名字为 `my_point`
2. `my_point` 包含两个 `double` 类型的成员变量 `x` 和 `y`

`UDT` 的核心代码如下，完整代码见 `code/udt_example`

```scala
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
```

原始数据定义如下，数据类型即为 `my_point`
```scala
val raw_data = Seq(
      Row(1, new my_point(1, 1), new my_point(10, 10)),
      Row(2, new my_point(2, 2), new my_point(20, 20)),
      Row(3, new my_point(3, 3), new my_point(30, 30)),
      Row(4, new my_point(4, 4), new my_point(40, 40)),
      Row(5, new my_point(5, 5), new my_point(50, 50))
    )
```

表结构定义如下，定义了数据类型为 `my_point_udt`
```scala
val sch = StructType(Array(
      StructField("idx", IntegerType, false),
      StructField("point1", new my_point_udt, false),
      StructField("point2", new my_point_udt, false)))
```

查询语句定义如下：
```scala
val test_sql = spark.sql("select idx, point1, point2  from data_test")
```

程序输出结果如下：
```text
+---+----------+------------+
|idx|    point1|      point2|
+---+----------+------------+
|  1|(1.0, 1.0)|(10.0, 10.0)|
|  2|(2.0, 2.0)|(20.0, 20.0)|
|  3|(3.0, 3.0)|(30.0, 30.0)|
|  4|(4.0, 4.0)|(40.0, 40.0)|
|  5|(5.0, 5.0)|(50.0, 50.0)|
+---+----------+------------+
```






