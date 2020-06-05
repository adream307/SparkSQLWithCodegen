# 自定义函数示例1

Spark 原生提供[UDF](https://docs.databricks.com/spark/latest/spark-sql/udf-scala.html)用于实现自定义函数

## UDF 示例
假设我们的需求是这样的：

- 需要一个名字为 `my_foo` 的函数
- 该函数接受两个 `double` 类型的参数作为输入，参数名记为 `x，y`
- 函数输出 `x*y+3` 。

为了实现上述功能，我们在程序中定义了 `my_foo` 匿名函数，并向 `spark` 注册 `udf`。完整代码位于 `code/udf_example1` 目录内
```scala
val my_foo = (x: Double, y: Double) => x * y + 3
spark.udf.register("my_foo", my_foo)
```
完成向 `spark` 注册 `udf` 后，可直接在 SQL 语句中调用该 `udf`， SQL 语句如下
```scala
val test_sql = spark.sql("select x, y, my_foo(x,y) from data_test")
```
程序运行结果如下
```text
+----+----+------------+
|   x|   y|my_foo(x, y)|
+----+----+------------+
| 1.0| 2.0|         5.0|
| 3.0| 4.0|        15.0|
| 5.0|null|        null|
|null| 6.0|        null|
|null|null|        null|
+----+----+------------+
```
根据程序运行结果，可以判断 `spark` 原生的 `udf` 机制，自动的帮用户处理了 `null` 输入，当 `my_foo` 的两个输入参数 `x,y`其中一个为 `null` 时，输出自动为 `null`。

关于这一点，可以在程序中添加如下代码，打印 SQL 语句的执行计划树 (Physical Plan)。
```scala
test_sql.explain()
```

输出执行计划树如下：
```text
== Physical Plan ==
*(1) Project [x#2, y#3, if ((isnull(x#2) OR isnull(y#3))) null else my_foo(knownnotnull(x#2), knownnotnull(y#3)) AS my_foo(x, y)#6]
+- *(1) Scan ExistingRDD[x#2,y#3]
```

根据执行计划树的 `if ((isnull(x#2) OR isnull(y#3))) null else my_foo(knownnotnull(x#2), knownnotnull(y#3))` 可以判断 `spark` 框架会判断输入的 `x,y` 均不为 `null` 时才调用 `my_foo` 函数
