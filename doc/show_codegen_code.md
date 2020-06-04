# 显示 Codegen 代码

在上一篇文章中，我们演示了如何新建一个 [Spark SQL 的工程](new_spark_sql_project.md)，边展示了一个简单的 `SQL` 查询

```sql
select x, y, power(x,y) from data_test
```
文章 [Deep Dive into Spark SQL’s Catalyst Optimizer](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)详细介绍了 Spark SQL 的优化机制。为了提高查询速度，Spark 会将查询的 SQL 语句动态生成生成一份对应 java 代码，直接运行该 java 代码以提高查询速度

