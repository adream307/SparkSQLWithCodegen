# 显示 Codegen 代码

在上一篇文章中，我们演示了如何新建一个 [Spark SQL 的工程](new_spark_sql_project.md)，并展示了一个简单的 `SQL` 查询

```sql
select x, y, power(x,y) from data_test
```
文章 [Deep Dive into Spark SQL’s Catalyst Optimizer](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html) 详细介绍了 Spark SQL 的优化机制。为了提高查询速度，Spark 会将查询的 SQL 语句动态生成一份对应的 java 代码。这个 java 代码和 SQL 语句是一一对应的，一个 SQL 语句对应这一份 java 代码。

## 查看 Codegen 代码

那如何查看这个动态生成的 java 代码呢？

Spark 提供了调试机制，可以将这个动态代码生成的 java 代码打印输出,在[上篇文章](new_spark_sql_project.md)的工程上，添加下列这行语句，即可动态打印输出该 java 代码
```scala
test_sql.queryExecution.debug.codegen()
```
完整代码位于 `code/show_codegen_code` 目录内


程序运行结果如下：
```text
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
20/06/04 07:39:42 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 1 WholeStageCodegen subtrees.
== Subtree 1 / 1 (maxMethodCodeSize:148; maxConstantPoolSize:118(0.18% used); numInnerClasses:0) ==
*(1) Project [x#2, y#3, POWER(x#2, y#3) AS POWER(x, y)#6]
+- *(1) Scan ExistingRDD[x#2,y#3]

Generated code:
/* 001 */ public Object generate(Object[] references) {
/* 002 */   return new GeneratedIteratorForCodegenStage1(references);
/* 003 */ }
/* 004 */
/* 005 */ // codegenStageId=1
/* 006 */ final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
/* 007 */   private Object[] references;
/* 008 */   private scala.collection.Iterator[] inputs;
/* 009 */   private scala.collection.Iterator rdd_input_0;
/* 010 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] rdd_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[3];
/* 011 */
/* 012 */   public GeneratedIteratorForCodegenStage1(Object[] references) {
/* 013 */     this.references = references;
/* 014 */   }
/* 015 */
/* 016 */   public void init(int index, scala.collection.Iterator[] inputs) {
/* 017 */     partitionIndex = index;
/* 018 */     this.inputs = inputs;
/* 019 */     rdd_input_0 = inputs[0];
/* 020 */     rdd_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
/* 021 */     rdd_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
/* 022 */     rdd_mutableStateArray_0[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 0);
/* 023 */
/* 024 */   }
/* 025 */
/* 026 */   private void project_doConsume_0(double project_expr_0_0, boolean project_exprIsNull_0_0, double project_expr_1_0, boolean project_exprIsNull_1_0) throws java.io.IOException {
/* 027 */     boolean project_isNull_4 = true;
/* 028 */     double project_value_4 = -1.0;
/* 029 */
/* 030 */     if (!project_exprIsNull_0_0) {
/* 031 */       if (!project_exprIsNull_1_0) {
/* 032 */         project_isNull_4 = false; // resultCode could change nullability.
/* 033 */         project_value_4 = java.lang.StrictMath.pow(project_expr_0_0, project_expr_1_0);
/* 034 */
/* 035 */       }
/* 036 */
/* 037 */     }
/* 038 */     rdd_mutableStateArray_0[2].reset();
/* 039 */
/* 040 */     rdd_mutableStateArray_0[2].zeroOutNullBytes();
/* 041 */
/* 042 */     if (project_exprIsNull_0_0) {
/* 043 */       rdd_mutableStateArray_0[2].setNullAt(0);
/* 044 */     } else {
/* 045 */       rdd_mutableStateArray_0[2].write(0, project_expr_0_0);
/* 046 */     }
/* 047 */
/* 048 */     if (project_exprIsNull_1_0) {
/* 049 */       rdd_mutableStateArray_0[2].setNullAt(1);
/* 050 */     } else {
/* 051 */       rdd_mutableStateArray_0[2].write(1, project_expr_1_0);
/* 052 */     }
/* 053 */
/* 054 */     if (project_isNull_4) {
/* 055 */       rdd_mutableStateArray_0[2].setNullAt(2);
/* 056 */     } else {
/* 057 */       rdd_mutableStateArray_0[2].write(2, project_value_4);
/* 058 */     }
/* 059 */     append((rdd_mutableStateArray_0[2].getRow()));
/* 060 */
/* 061 */   }
/* 062 */
/* 063 */   protected void processNext() throws java.io.IOException {
/* 064 */     while ( rdd_input_0.hasNext()) {
/* 065 */       InternalRow rdd_row_0 = (InternalRow) rdd_input_0.next();
/* 066 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
/* 067 */       boolean rdd_isNull_0 = rdd_row_0.isNullAt(0);
/* 068 */       double rdd_value_0 = rdd_isNull_0 ?
/* 069 */       -1.0 : (rdd_row_0.getDouble(0));
/* 070 */       boolean rdd_isNull_1 = rdd_row_0.isNullAt(1);
/* 071 */       double rdd_value_1 = rdd_isNull_1 ?
/* 072 */       -1.0 : (rdd_row_0.getDouble(1));
/* 073 */
/* 074 */       project_doConsume_0(rdd_value_0, rdd_isNull_0, rdd_value_1, rdd_isNull_1);
/* 075 */       if (shouldStop()) return;
/* 076 */     }
/* 077 */   }
/* 078 */
/* 079 */ }


+----+----+-----------+
|   x|   y|POWER(x, y)|
+----+----+-----------+
| 1.0| 2.0|        1.0|
| 3.0| 4.0|       81.0|
| 5.0|null|       null|
|null| 6.0|       null|
|null|null|       null|
+----+----+-----------+
```

## Codegen 代码分析

在上述的 Codegen 代码的第 33 行，可以找到如下语句，该语句即为 SQL 语句中 `power` 函数的具体实现

```java
project_value_4 = java.lang.StrictMath.pow(project_expr_0_0, project_expr_1_0);
```

可以将本例子的 SQL 语句改成如下语句

```sql
select x, y, sqrt(x+y) from data_test
```
那么在动态生成的 java 代码中一定可以找到如下代码，用于实现 SQL 语句中的 `sqrt` 函数

```java
project_value_4 = java.lang.Math.sqrt(project_value_5);
```