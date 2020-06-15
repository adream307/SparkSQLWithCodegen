# 自定义数据类型上的自定义函数
在[上篇文章](udt_example.md)中，我们介绍了自定义数据类型，在本篇文章中，我们介绍如何在自定义数据类型上定义自定义函数，参考前文介绍的 `my_foo` 方法，这里我们介绍两种非 `codegen` 实现的自定义函数

## 自定义函数
自定义函数的需求如下:

- 该函数接受两个 `my_point` 类型的参数作为输入
- 参数名记为 `x，y`
- 函数输出 `my_point(x.x+y.x, x.y+y.y)`

### 方法1
使用 `SparkSQL` 内置的 `UDF` 实现，核心代码如下：
```scala
val my_foo1 = (x: my_point, y: my_point) => {
   if ((x == null) || (y == null)) null
   else new my_point(x.x + y.x, x.y + y.y)
}
spark.udf.register("my_foo1", my_foo1)
```
和[前文](./udf_example1.md)略有不同，`UDT` 上的自定义函数需要用户在 `UDF` 内判断 `null`， `SparkSQL` 框架并不帮用户处理空值

### 方法2
和[前文](./udf_example2.md)类似，该方法从 `Expression` 继承并实现 `eval` 函数

```scala
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
```
相应的，该方法也需要注册
```scala
spark.sessionState.functionRegistry.createOrReplaceTempFunction("my_foo2", my_foo2)
```

## 程序输出
完整代码见 `code/udt_udf_example`。

方法1 输出效果如下：
```text
+---+----------+------------+-----------------------+
|idx|    point1|      point2|my_foo1(point1, point2)|
+---+----------+------------+-----------------------+
|  1|(1.0, 1.0)|(10.0, 10.0)|           (11.0, 11.0)|
|  2|(2.0, 2.0)|(20.0, 20.0)|           (22.0, 22.0)|
|  3|      null|(30.0, 30.0)|                   null|
|  4|(4.0, 4.0)|        null|                   null|
|  5|      null|        null|                   null|
+---+----------+------------+-----------------------+
```
方法2 输出效果如下:
```text
+---+----------+------------+-----------------------+
|idx|    point1|      point2|my_foo2(point1, point2)|
+---+----------+------------+-----------------------+
|  1|(1.0, 1.0)|(10.0, 10.0)|           (11.0, 11.0)|
|  2|(2.0, 2.0)|(20.0, 20.0)|           (22.0, 22.0)|
|  3|      null|(30.0, 30.0)|                   null|
|  4|(4.0, 4.0)|        null|                   null|
|  5|      null|        null|                   null|
+---+----------+------------+-----------------------+
```