# Codegen 示例

在之前的文章中，我们介绍了 Spark 中两种自定义函数的实现，本篇文章中我们将正式介绍用 Codegen 方式实现的自定义函数。为了简化叙述，本篇文章并不介绍如何在 Codegen 中处理 Null 值，假设用户的所有输入数据均为非空。


## Codegen 的自定义函数

自定义函数的需求与前文一样：

- 需要一个名字为 `my_foo` 的函数
- 该函数接受两个 `double` 类型的参数作为输入
- 参数名记为 `x，y`
- 函数输出 `x*y+3` 

与非 UDF 的自定义函数类似，Codegen 实现的自定义函数也需要从 `Expression` 继承，完整的核心代码如下:
```scala
package org.apache.spark.sql.myfunctions {

  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions._
  import org.apache.spark.sql.catalyst.expressions.codegen._
  import org.apache.spark.sql.catalyst.expressions.codegen.Block._

  case class my_foo(inputExpressions: Seq[Expression]) extends Expression with ExpectsInputTypes {
    override def nullable: Boolean = false

    override def eval(input: InternalRow): Any = ???

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val left_code = inputExpressions(0).genCode(ctx)
      val right_code = inputExpressions(1).genCode(ctx)
      ev.copy(code =
        code"""
              | ${left_code.code}
              | ${right_code.code}
              |
              | ${CodeGenerator.javaType(DoubleType)} ${ev.value} = ${left_code.value} * ${right_code.value};
              | ${ev.value} = ${ev.value} + 3;
              |""".stripMargin, FalseLiteral)
    }

    override def dataType: DataType = DoubleType

    override def children: Seq[Expression] = inputExpressions

    override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)
  }

}
```

## 程序说明
1. 因为采用 Codegen 方式实现，所以不需要 `CodegenFallback` 特征
2. 因为确保输入数据不为 `null` ，所以 `my_foo` 函数输出肯定不为 `null`，所以 `nullable` 为 `false`
3. 因为采用 Codegen 方式实现，所以不需要实现 `eval` 方法
4. `doGenCode` 方法为 Codegen 的核心代码
5. `left_code = inputExpressions(0).genCode(ctx)` 生成 `my_foo` 第一个参数的 Codegen 代码
6. `right_code = inputExpressions(1).genCode(ctx)` 生成 `my_foo` 第二个参数的 Codegen 代码
7. `doGenCode` 方法中需要声明一个名为 `${ev.value}` 的变量，并对该变量赋值，该变量为即为 `my_foo`最后输出结果
8. 下述代码实现 `x*y+3` 的计算，并将结果赋值给 `${ev.value}`
```
${CodeGenerator.javaType(DoubleType)} ${ev.value} = ${left_code.value} * ${right_code.value};
${ev.value} = ${ev.value} + 3;
```
9. 因为当前函数不处理 `null`, 所以 `ev.copy`的第二个参数为 `FalseLiteral`


## 注册 my_foo
与`非UDF的自定义函数`一样，`Codgegen`实现的自定义函数使用同样的方法注册
```scala
import org.apache.spark.sql.myfunctions.my_foo
spark.sessionState.functionRegistry.createOrReplaceTempFunction("my_foo", my_foo)
```

## 运行程序

完整代码位于 `code/codegen_example` 目录内，程序运行效果如下：
```text
+---+---+------------+
|  x|  y|my_foo(x, y)|
+---+---+------------+
|1.0|2.0|         5.0|
|3.0|4.0|        15.0|
+---+---+------------+
```

Codegen 生成的完整 java 代码如下，其中第 `27～28` 行即为 `my_foo` 函数的具体实现
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



