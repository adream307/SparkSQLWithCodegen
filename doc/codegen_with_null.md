# 如何处理 Null

在上篇文章我，我们介绍了 Codegen 实现的自定义函数，当时为了简化叙述，假设所有输入数据均为非空值。在本篇文章中，我们将介绍如何在 Codegen 中处理空值的问题。


## Codegen 的自定义函数

自定义函数的需求与前文一样：

- 需要一个名字为 `my_foo` 的函数
- 该函数接受两个 `double` 类型的参数作为输入
- 参数名记为 `x，y`
- 函数输出 `x*y+3` 

与不处理 Null 的自定义函数有两处区别： `nullable` 和 `doGenCode`， 程序核心代码如下：
```scala
package org.apache.spark.sql.myfunctions {

  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions._
  import org.apache.spark.sql.catalyst.expressions.codegen._
  import org.apache.spark.sql.catalyst.expressions.codegen.Block._

  case class my_foo(inputExpressions: Seq[Expression]) extends Expression with ExpectsInputTypes {
    override def nullable: Boolean = inputExpressions(0).nullable || inputExpressions(1).nullable

    override def eval(input: InternalRow): Any = ???

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val left_expr = inputExpressions(0)
      val right_expr = inputExpressions(1)
      val left_code = left_expr.genCode(ctx)
      val right_code = right_expr.genCode(ctx)

      if (nullable) {
        val nullSafeEval =
          left_code.code + ctx.nullSafeExec(left_expr.nullable, left_code.isNull) {
            right_code.code + ctx.nullSafeExec(right_expr.nullable, right_code.isNull) {
              s"""
                 | ${ev.isNull} = false; // resultCode could change nullability.
                 | ${ev.value} = ${left_code.value} * ${right_code.value};
                 | ${ev.value} = ${ev.value} + 3;
                 |""".stripMargin
            }
          }
        ev.copy(code =
          code"""
             boolean ${ev.isNull} = true;
             ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
             $nullSafeEval
              """)
      } else {
        ev.copy(code =
          code"""
             ${left_code.code}
             ${right_code.code}
             ${CodeGenerator.javaType(dataType)} ${ev.value} = ${left_code.value} * ${right_code.value};
             ${ev.value} = ${ev.value} + 3;
              """, FalseLiteral)
      }
    }

    override def dataType: DataType = DoubleType

    override def children: Seq[Expression] = inputExpressions

    override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)
  }

}
```

## 程序说明
1. `nullable` 不再是简单的 `true` 或 `false` ，而是根据输入参数确定，只要输入参数有一个允许为空，那么输出就允许为空
```scala
override def nullable: Boolean = inputExpressions(0).nullable || inputExpressions(1).nullable
```
2. 根据 `nullable` 的值，`doGenCode` 分为两部分
3. 当 `nullable` 为 `false` 时，`doGenCode` 的代码与[上篇文章](./codegen_example.md) 完全一致
4. 当 `nullable` 为 `true` 时，程序在 codegn 代码中首先判断输入均不为 `null` 之后再做 `my_foo` 的函数运算
```scala
left_code.code + ctx.nullSafeExec(left_expr.nullable, left_code.isNull) {
  right_code.code + ctx.nullSafeExec(right_expr.nullable, right_code.isNull) {
    s"""
     | ${ev.isNull} = false; // resultCode could change nullability.
     | ${ev.value} = ${left_code.value} * ${right_code.value};
     | ${ev.value} = ${ev.value} + 3;
     |""".stripMargin
  }
}
```
5. 当 `nullable` 为 `true` 时，codegen 代码需要定义 `bool` 变量 `${ev.isNull}` ，并对其赋值，表示本次计算输出结果是否为 `null`


## 运行程序

完整代码位于 `code/codegen_with_null` 目录内，示例代码中包含两处查询，分别对应 `nullable` 分别为 `true` 和 `false` 的查询代码。

### 第一次查询
第一次查询定义的表结构如下，即输入数据 `x` 和 `y` 均允许为空，那么 `my_foo` 的输出亦允许为空
```scala
val sch = StructType(Array(StructField("x", DoubleType, true), StructField("y", DoubleType, true)))
```
程序输出效果如下：
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

Codegen 生成的完整 java 代码如下，其中第 `30～38` 行即为 `my_foo` 函数对 `null` 值的处理。
```text
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
/* 033 */         project_value_4 = project_expr_0_0 * project_expr_1_0;
/* 034 */         project_value_4 = project_value_4 + 3;
/* 035 */
/* 036 */       }
/* 037 */
/* 038 */     }
/* 039 */     rdd_mutableStateArray_0[2].reset();
/* 040 */
/* 041 */     rdd_mutableStateArray_0[2].zeroOutNullBytes();
/* 042 */
/* 043 */     if (project_exprIsNull_0_0) {
/* 044 */       rdd_mutableStateArray_0[2].setNullAt(0);
/* 045 */     } else {
/* 046 */       rdd_mutableStateArray_0[2].write(0, project_expr_0_0);
/* 047 */     }
/* 048 */
/* 049 */     if (project_exprIsNull_1_0) {
/* 050 */       rdd_mutableStateArray_0[2].setNullAt(1);
/* 051 */     } else {
/* 052 */       rdd_mutableStateArray_0[2].write(1, project_expr_1_0);
/* 053 */     }
/* 054 */
/* 055 */     if (project_isNull_4) {
/* 056 */       rdd_mutableStateArray_0[2].setNullAt(2);
/* 057 */     } else {
/* 058 */       rdd_mutableStateArray_0[2].write(2, project_value_4);
/* 059 */     }
/* 060 */     append((rdd_mutableStateArray_0[2].getRow()));
/* 061 */
/* 062 */   }
/* 063 */
/* 064 */   protected void processNext() throws java.io.IOException {
/* 065 */     while ( rdd_input_0.hasNext()) {
/* 066 */       InternalRow rdd_row_0 = (InternalRow) rdd_input_0.next();
/* 067 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
/* 068 */       boolean rdd_isNull_0 = rdd_row_0.isNullAt(0);
/* 069 */       double rdd_value_0 = rdd_isNull_0 ?
/* 070 */       -1.0 : (rdd_row_0.getDouble(0));
/* 071 */       boolean rdd_isNull_1 = rdd_row_0.isNullAt(1);
/* 072 */       double rdd_value_1 = rdd_isNull_1 ?
/* 073 */       -1.0 : (rdd_row_0.getDouble(1));
/* 074 */
/* 075 */       project_doConsume_0(rdd_value_0, rdd_isNull_0, rdd_value_1, rdd_isNull_1);
/* 076 */       if (shouldStop()) return;
/* 077 */     }
/* 078 */   }
/* 079 */
/* 080 */ }

```

### 第二次查询
第二次查询定义的表结构如下，即输入数据 `x` 和 `y` 均不为空，那么 `my_foo` 的输出亦不为空
```scala
val sch2 = StructType(Array(StructField("x", DoubleType, false), StructField("y", DoubleType, false)))
```
程序输出效果如下：
```text
+---+----+------------+
|  x|   y|my_foo(x, y)|
+---+----+------------+
|1.0| 2.0|         5.0|
|3.0| 4.0|        15.0|
|5.0| 6.0|        33.0|
|7.0| 8.0|        59.0|
|9.0|10.0|        93.0|
+---+----+------------+
```

Codegen 生成的完整 java 代码如下，与[上篇文章](./codegen_example.md)的代码完全一致。
```text
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
/* 026 */   private void project_doConsume_0(double project_expr_0_0, double project_expr_1_0) throws java.io.IOException {
/* 027 */     double project_value_4 = project_expr_0_0 * project_expr_1_0;
/* 028 */     project_value_4 = project_value_4 + 3;
/* 029 */     rdd_mutableStateArray_0[2].reset();
/* 030 */
/* 031 */     rdd_mutableStateArray_0[2].write(0, project_expr_0_0);
/* 032 */
/* 033 */     rdd_mutableStateArray_0[2].write(1, project_expr_1_0);
/* 034 */
/* 035 */     rdd_mutableStateArray_0[2].write(2, project_value_4);
/* 036 */     append((rdd_mutableStateArray_0[2].getRow()));
/* 037 */
/* 038 */   }
/* 039 */
/* 040 */   protected void processNext() throws java.io.IOException {
/* 041 */     while ( rdd_input_0.hasNext()) {
/* 042 */       InternalRow rdd_row_0 = (InternalRow) rdd_input_0.next();
/* 043 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
/* 044 */       double rdd_value_0 = rdd_row_0.getDouble(0);
/* 045 */       double rdd_value_1 = rdd_row_0.getDouble(1);
/* 046 */
/* 047 */       project_doConsume_0(rdd_value_0, rdd_value_1);
/* 048 */       if (shouldStop()) return;
/* 049 */     }
/* 050 */   }
/* 051 */
/* 052 */ }
```
