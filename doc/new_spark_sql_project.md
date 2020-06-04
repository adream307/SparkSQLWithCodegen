# 新建一个 Spark SQL 的工程

本文介绍如何新建一个 Spark SQL 的例子，[Spark](https://spark.apache.org/) 采用 standalone 的 local 模式, 需要安装 [scala](https://www.scala-lang.org/download/)

本文体所演示的例子位于 `code/new_spark_sql_project` 目录内

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

## 编写spark程序
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

    val test_sql = spark.sql("select x, y, pow(x,y) from data_test")
    test_sql.show()
    spark.stop()

  }

}
```

