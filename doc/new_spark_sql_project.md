# 新建一个 Spark SQL 的工程

本文介绍如何新建一个 Spark SQL 的例子，[Spark](https://spark.apache.org/) 采用 standalone 的 local 模式, 需要安装 [scala](https://www.scala-lang.org/download/)

本文所演示的例子位于 `code/new_spark_sql_project` 目录内

## 新建目录结构
Spark SQL 工程的目录结构如下所示:
```
new_spark_sql_project/
|-- build.sbt
`-- src
    `-- main
        `-- scala
            `-- SparkSQLTest.scala
```

## 添加 scala 依赖库
编辑 `new_spark_sql_project/build.sbt` 内容如下
```scala
name := "SparkSQLTest"
  
version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-preview2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0-preview2"
```

build.sbt 文件指定了 scala 的版本为 2.12.11 , spark 版本为 3.0.0-preview2

## 编写 Spark 程序
编辑 `new_spark_sql_project/src/main/scala/SparkSQLTest.scala` 内容如下
```scala
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
    test_sql.show()
    spark.stop()

  }

}
```

## 运行 Spark 程序

进入 `new_spark_sql_project/` 根目录，执行一下命令
```bash
sbt run
```
输出结果如下:
```text
[info] running SparkSQLTest 
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
20/06/04 06:36:33 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
+----+----+-----------+
|   x|   y|POWER(x, y)|
+----+----+-----------+
| 1.0| 2.0|        1.0|
| 3.0| 4.0|       81.0|
| 5.0|null|       null|
|null| 6.0|       null|
|null|null|       null|
+----+----+-----------+

[success] Total time: 11 s, completed Jun 4, 2020 6:36:38 AM
```

## Spark 程序讲解

```scala
Logger.getLogger("org").setLevel(Level.WARN)
```
设置当前日志级别为`WARN`，默认的日志级别为`INFO`

---

```scala
val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark sql test")
      .getOrCreate()
```
新建 SparkSession， 并设置其运行方式为 standalone 的 local 模式

---

```scala
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
```
在 spark 内新建一个临时表 `data_test`，内容如下
x | y
---|---
1.0 | 2.0
3.0 | 4.0
5.0 | null
null | 6.0
null | null
其中 `x`,`y`的数据类型均为 `double`

---

```scala
val test_sql = spark.sql("select x, y, pow(x,y) from data_test")
test_sql.show()
```
在临时表 `data_test` 上查询数据，结果如下
x | y | POWER(x, y)
---|---|---
1.0| 2.0| 1.0|
3.0| 4.0| 81.0|
5.0|null| null|
null| 6.0| null|
null|null| null|

---

```scala
spark.stop()
```
结果当前 Spark 任务并
