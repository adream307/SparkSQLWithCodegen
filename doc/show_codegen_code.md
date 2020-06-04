# 显示 Codegen 代码

在上一篇文章中，我们演示了如何新建一个 [Spark SQL 的工程](new_spark_sql_project.md)，边展示了一个简单的 `SQL` 查询

```sql
select x, y, power(x,y) from data_test
```
文章 [Deep Dive into Spark SQL’s Catalyst Optimizer](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)详细介绍了 Spark SQL 的优化机制。为了提高查询速度，Spark 会将查询的 SQL 语句动态生成生成一份对应 java 代码。这个 java 代码和 SQL 语句是一一对应的，一个 SQL 语句对应这一份 java 代码。

那如何查看这个动态生成的 java 代码呢？

Spark 提供了调试机制，可以将这个动态代码生成的 java 代码打印处理啊,在[上篇文章](new_spark_sql_project.md)的工程上，添加下列这行语句，即可动态打印输出该 java 代码
```scala
test_sql.queryExecution.debug.codegen()
```
完整代码位于 `code/show_codegen_code` 目录内


程序运行结果如下：
```text

```

