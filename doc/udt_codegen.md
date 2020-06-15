# 自定义数据类型上的 Codegen
本篇文章是这个系列的最后一篇文章，我们介绍如何在 `UDT` 上做 `Codegen`，和[前文](./udt_udf_example.md)类似， `Codegen` 实现的自定义函数要求如下:
- 该函数接受两个 `my_point` 类型的参数作为输入
- 参数名记为 `x，y`
- 函数输出 `my_point(x.x+y.x, x.y+y.y)`

## Codegen
和[前文](./codegen_example.md)类似， `Codegen` 实现的自定义函数也是从 `Expression` 继承，并实现 `doGenCode` 方法。`doGenCode` 也按照 `nullable` 的值分为两部分，完整核心代码如下：
```scala
case class my_foo(inputExpr: Seq[Expression]) extends Expression with ExpectsInputTypes {
  override def nullable: Boolean = inputExpr(0).nullable || inputExpr(1).nullable

  override def eval(input: InternalRow): Any = ???

  override def dataType: DataType = new my_point_udt

  override def inputTypes: Seq[AbstractDataType] = Seq(new my_point_udt, new my_point_udt)

  override def children: Seq[Expression] = inputExpr

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    import org.apache.spark.sql.catalyst.expressions.codegen._
    import org.apache.spark.sql.catalyst.expressions.codegen.Block._

    val left_expr = inputExpr(0)
    val right_expr = inputExpr(1)
    val left_code = left_expr.genCode(ctx)
    val right_code = right_expr.genCode(ctx)

    if (nullable) {
      val nullSafeEval =
        left_code.code + ctx.nullSafeExec(left_expr.nullable, left_code.isNull) {
          right_code.code + ctx.nullSafeExec(right_expr.nullable, right_code.isNull) {
            s"""
               |${ev.isNull} = false; // resultCode could change nullability.
               |${CodeGenerator.javaType(DoubleType)} ${ev.value}_x = ${left_code.value}.getDouble(0) + ${right_code.value}.getDouble(0);
               |${CodeGenerator.javaType(DoubleType)} ${ev.value}_y = ${left_code.value}.getDouble(1) + ${right_code.value}.getDouble(1);
               |org.apache.spark.sql.myfunctions.my_point ${ev.value}_p = new org.apache.spark.sql.myfunctions.my_point(${ev.value}_x, ${ev.value}_y);
               |org.apache.spark.sql.myfunctions.my_point_udt ${ev.value}_u = new org.apache.spark.sql.myfunctions.my_point_udt();
               |${ev.value} = ${ev.value}_u.serialize(${ev.value}_p);
               |""".stripMargin
          }
        }
      ev.copy(code =
        code"""
              boolean ${ev.isNull} = true;
              ${CodeGenerator.javaType(ArrayType(DoubleType, false))} ${ev.value} = null;
              $nullSafeEval
            """
      )
    } else {
      ev.copy(code =
        code"""
              ${left_code.code}
              ${right_code.code}
              ${CodeGenerator.javaType(DoubleType)} ${ev.value}_x = ${left_code.value}.getDouble(0) + ${right_code.value}.getDouble(0);
              ${CodeGenerator.javaType(DoubleType)} ${ev.value}_y = ${left_code.value}.getDouble(1) + ${right_code.value}.getDouble(1);
              org.apache.spark.sql.myfunctions.my_point ${ev.value}_p = new org.apache.spark.sql.myfunctions.my_point(${ev.value}_x, ${ev.value}_y);
              org.apache.spark.sql.myfunctions.my_point_udt ${ev.value}_u = new org.apache.spark.sql.myfunctions.my_point_udt();
              ${CodeGenerator.javaType(ArrayType(DoubleType, false))} ${ev.value} = ${ev.value}_u.serialize(${ev.value}_p);
            """, FalseLiteral)
    }
  }
}
```
和[前文](./codegen_example.md) `SparkSQL` 内置数据类型的 `Codegen` 略有不同，在 `UDT` 的 `Codegen` 中, 最后赋值给变量 `${ev.value}` 的是 `my_point` 的序列化，而不是 `my_point`。

## 程序输出
完整代码见 `code/udt_codegen`，处理 `null` 的 Codegen 代码如下：
```java
/* 001 */ public Object generate(Object[] references) {
/* 002 */   return new GeneratedIteratorForCodegenStage1(references);
/* 003 */ }
/* 004 */
/* 005 */ // codegenStageId=1
/* 006 */ final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
/* 007 */   private Object[] references;
/* 008 */   private scala.collection.Iterator[] inputs;
/* 009 */   private scala.collection.Iterator rdd_input_0;
/* 010 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] rdd_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];
/* 011 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter[] rdd_mutableStateArray_1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter[5];
/* 012 */
/* 013 */   public GeneratedIteratorForCodegenStage1(Object[] references) {
/* 014 */     this.references = references;
/* 015 */   }
/* 016 */
/* 017 */   public void init(int index, scala.collection.Iterator[] inputs) {
/* 018 */     partitionIndex = index;
/* 019 */     this.inputs = inputs;
/* 020 */     rdd_input_0 = inputs[0];
/* 021 */     rdd_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 64);
/* 022 */     rdd_mutableStateArray_1[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[0], 8);
/* 023 */     rdd_mutableStateArray_1[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[0], 8);
/* 024 */     rdd_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 96);
/* 025 */     rdd_mutableStateArray_1[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[1], 8);
/* 026 */     rdd_mutableStateArray_1[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[1], 8);
/* 027 */     rdd_mutableStateArray_1[4] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[1], 8);
/* 028 */
/* 029 */   }
/* 030 */
/* 031 */   protected void processNext() throws java.io.IOException {
/* 032 */     while ( rdd_input_0.hasNext()) {
/* 033 */       InternalRow rdd_row_0 = (InternalRow) rdd_input_0.next();
/* 034 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
/* 035 */       boolean rdd_isNull_1 = rdd_row_0.isNullAt(1);
/* 036 */       ArrayData rdd_value_1 = rdd_isNull_1 ?
/* 037 */       null : (rdd_row_0.getArray(1));
/* 038 */       boolean rdd_isNull_2 = rdd_row_0.isNullAt(2);
/* 039 */       ArrayData rdd_value_2 = rdd_isNull_2 ?
/* 040 */       null : (rdd_row_0.getArray(2));
/* 041 */
/* 042 */       int rdd_value_0 = rdd_row_0.getInt(0);
/* 043 */       boolean project_isNull_3 = true;
/* 044 */       ArrayData project_value_3 = null;
/* 045 */
/* 046 */       if (!rdd_isNull_1) {
/* 047 */         if (!rdd_isNull_2) {
/* 048 */           project_isNull_3 = false; // resultCode could change nullability.
/* 049 */           double project_value_3_x = rdd_value_1.getDouble(0) + rdd_value_2.getDouble(0);
/* 050 */           double project_value_3_y = rdd_value_1.getDouble(1) + rdd_value_2.getDouble(1);
/* 051 */           org.apache.spark.sql.myfunctions.my_point project_value_3_p = new org.apache.spark.sql.myfunctions.my_point(project_value_3_x, project_value_3_y);
/* 052 */           org.apache.spark.sql.myfunctions.my_point_udt project_value_3_u = new org.apache.spark.sql.myfunctions.my_point_udt();
/* 053 */           project_value_3 = project_value_3_u.serialize(project_value_3_p);
/* 054 */
/* 055 */         }
/* 056 */
/* 057 */       }
/* 058 */       rdd_mutableStateArray_0[1].reset();
/* 059 */
/* 060 */       rdd_mutableStateArray_0[1].zeroOutNullBytes();
/* 061 */
/* 062 */       rdd_mutableStateArray_0[1].write(0, rdd_value_0);
/* 063 */
/* 064 */       if (rdd_isNull_1) {
/* 065 */         rdd_mutableStateArray_0[1].setNullAt(1);
/* 066 */       } else {
/* 067 */         // Remember the current cursor so that we can calculate how many bytes are
/* 068 */         // written later.
/* 069 */         final int project_previousCursor_0 = rdd_mutableStateArray_0[1].cursor();
/* 070 */
/* 071 */         final ArrayData project_tmpInput_0 = rdd_value_1;
/* 072 */         if (project_tmpInput_0 instanceof UnsafeArrayData) {
/* 073 */           rdd_mutableStateArray_0[1].write((UnsafeArrayData) project_tmpInput_0);
/* 074 */         } else {
/* 075 */           final int project_numElements_0 = project_tmpInput_0.numElements();
/* 076 */           rdd_mutableStateArray_1[2].initialize(project_numElements_0);
/* 077 */
/* 078 */           for (int project_index_0 = 0; project_index_0 < project_numElements_0; project_index_0++) {
/* 079 */             rdd_mutableStateArray_1[2].write(project_index_0, project_tmpInput_0.getDouble(project_index_0));
/* 080 */           }
/* 081 */         }
/* 082 */
/* 083 */         rdd_mutableStateArray_0[1].setOffsetAndSizeFromPreviousCursor(1, project_previousCursor_0);
/* 084 */       }
/* 085 */
/* 086 */       if (rdd_isNull_2) {
/* 087 */         rdd_mutableStateArray_0[1].setNullAt(2);
/* 088 */       } else {
/* 089 */         // Remember the current cursor so that we can calculate how many bytes are
/* 090 */         // written later.
/* 091 */         final int project_previousCursor_1 = rdd_mutableStateArray_0[1].cursor();
/* 092 */
/* 093 */         final ArrayData project_tmpInput_1 = rdd_value_2;
/* 094 */         if (project_tmpInput_1 instanceof UnsafeArrayData) {
/* 095 */           rdd_mutableStateArray_0[1].write((UnsafeArrayData) project_tmpInput_1);
/* 096 */         } else {
/* 097 */           final int project_numElements_1 = project_tmpInput_1.numElements();
/* 098 */           rdd_mutableStateArray_1[3].initialize(project_numElements_1);
/* 099 */
/* 100 */           for (int project_index_1 = 0; project_index_1 < project_numElements_1; project_index_1++) {
/* 101 */             rdd_mutableStateArray_1[3].write(project_index_1, project_tmpInput_1.getDouble(project_index_1));
/* 102 */           }
/* 103 */         }
/* 104 */
/* 105 */         rdd_mutableStateArray_0[1].setOffsetAndSizeFromPreviousCursor(2, project_previousCursor_1);
/* 106 */       }
/* 107 */
/* 108 */       if (project_isNull_3) {
/* 109 */         rdd_mutableStateArray_0[1].setNullAt(3);
/* 110 */       } else {
/* 111 */         // Remember the current cursor so that we can calculate how many bytes are
/* 112 */         // written later.
/* 113 */         final int project_previousCursor_2 = rdd_mutableStateArray_0[1].cursor();
/* 114 */
/* 115 */         final ArrayData project_tmpInput_2 = project_value_3;
/* 116 */         if (project_tmpInput_2 instanceof UnsafeArrayData) {
/* 117 */           rdd_mutableStateArray_0[1].write((UnsafeArrayData) project_tmpInput_2);
/* 118 */         } else {
/* 119 */           final int project_numElements_2 = project_tmpInput_2.numElements();
/* 120 */           rdd_mutableStateArray_1[4].initialize(project_numElements_2);
/* 121 */
/* 122 */           for (int project_index_2 = 0; project_index_2 < project_numElements_2; project_index_2++) {
/* 123 */             rdd_mutableStateArray_1[4].write(project_index_2, project_tmpInput_2.getDouble(project_index_2));
/* 124 */           }
/* 125 */         }
/* 126 */
/* 127 */         rdd_mutableStateArray_0[1].setOffsetAndSizeFromPreviousCursor(3, project_previousCursor_2);
/* 128 */       }
/* 129 */       append((rdd_mutableStateArray_0[1].getRow()));
/* 130 */       if (shouldStop()) return;
/* 131 */     }
/* 132 */   }
/* 133 */
/* 134 */ }
``` 
其中 `46~57` 包含 `my_foo` 对空值的处理，程序输出结果如下：
```text
+---+----------+------------+----------------------+
|idx|    point1|      point2|my_foo(point1, point2)|
+---+----------+------------+----------------------+
|  1|(1.0, 1.0)|(10.0, 10.0)|          (11.0, 11.0)|
|  2|(2.0, 2.0)|(20.0, 20.0)|          (22.0, 22.0)|
|  3|      null|(30.0, 30.0)|                  null|
|  4|(4.0, 4.0)|        null|                  null|
|  5|      null|        null|                  null|
+---+----------+------------+----------------------+
```


不处理 `null` 的 Codegen 代码如下，其中 `39 ~ 43` 为 `my_foo` 的核心代码
```java
/* 001 */ public Object generate(Object[] references) {
/* 002 */   return new GeneratedIteratorForCodegenStage1(references);
/* 003 */ }
/* 004 */
/* 005 */ // codegenStageId=1
/* 006 */ final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
/* 007 */   private Object[] references;
/* 008 */   private scala.collection.Iterator[] inputs;
/* 009 */   private scala.collection.Iterator rdd_input_0;
/* 010 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] rdd_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];
/* 011 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter[] rdd_mutableStateArray_1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter[5];
/* 012 */
/* 013 */   public GeneratedIteratorForCodegenStage1(Object[] references) {
/* 014 */     this.references = references;
/* 015 */   }
/* 016 */
/* 017 */   public void init(int index, scala.collection.Iterator[] inputs) {
/* 018 */     partitionIndex = index;
/* 019 */     this.inputs = inputs;
/* 020 */     rdd_input_0 = inputs[0];
/* 021 */     rdd_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 64);
/* 022 */     rdd_mutableStateArray_1[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[0], 8);
/* 023 */     rdd_mutableStateArray_1[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[0], 8);
/* 024 */     rdd_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 96);
/* 025 */     rdd_mutableStateArray_1[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[1], 8);
/* 026 */     rdd_mutableStateArray_1[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[1], 8);
/* 027 */     rdd_mutableStateArray_1[4] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter(rdd_mutableStateArray_0[1], 8);
/* 028 */
/* 029 */   }
/* 030 */
/* 031 */   protected void processNext() throws java.io.IOException {
/* 032 */     while ( rdd_input_0.hasNext()) {
/* 033 */       InternalRow rdd_row_0 = (InternalRow) rdd_input_0.next();
/* 034 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
/* 035 */       ArrayData rdd_value_1 = rdd_row_0.getArray(1);
/* 036 */       ArrayData rdd_value_2 = rdd_row_0.getArray(2);
/* 037 */
/* 038 */       int rdd_value_0 = rdd_row_0.getInt(0);
/* 039 */       double project_value_3_x = rdd_value_1.getDouble(0) + rdd_value_2.getDouble(0);
/* 040 */       double project_value_3_y = rdd_value_1.getDouble(1) + rdd_value_2.getDouble(1);
/* 041 */       org.apache.spark.sql.myfunctions.my_point project_value_3_p = new org.apache.spark.sql.myfunctions.my_point(project_value_3_x, project_value_3_y);
/* 042 */       org.apache.spark.sql.myfunctions.my_point_udt project_value_3_u = new org.apache.spark.sql.myfunctions.my_point_udt();
/* 043 */       ArrayData project_value_3 = project_value_3_u.serialize(project_value_3_p);
/* 044 */       rdd_mutableStateArray_0[1].reset();
/* 045 */
/* 046 */       rdd_mutableStateArray_0[1].write(0, rdd_value_0);
/* 047 */
/* 048 */       // Remember the current cursor so that we can calculate how many bytes are
/* 049 */       // written later.
/* 050 */       final int project_previousCursor_0 = rdd_mutableStateArray_0[1].cursor();
/* 051 */
/* 052 */       final ArrayData project_tmpInput_0 = rdd_value_1;
/* 053 */       if (project_tmpInput_0 instanceof UnsafeArrayData) {
/* 054 */         rdd_mutableStateArray_0[1].write((UnsafeArrayData) project_tmpInput_0);
/* 055 */       } else {
/* 056 */         final int project_numElements_0 = project_tmpInput_0.numElements();
/* 057 */         rdd_mutableStateArray_1[2].initialize(project_numElements_0);
/* 058 */
/* 059 */         for (int project_index_0 = 0; project_index_0 < project_numElements_0; project_index_0++) {
/* 060 */           rdd_mutableStateArray_1[2].write(project_index_0, project_tmpInput_0.getDouble(project_index_0));
/* 061 */         }
/* 062 */       }
/* 063 */
/* 064 */       rdd_mutableStateArray_0[1].setOffsetAndSizeFromPreviousCursor(1, project_previousCursor_0);
/* 065 */
/* 066 */       // Remember the current cursor so that we can calculate how many bytes are
/* 067 */       // written later.
/* 068 */       final int project_previousCursor_1 = rdd_mutableStateArray_0[1].cursor();
/* 069 */
/* 070 */       final ArrayData project_tmpInput_1 = rdd_value_2;
/* 071 */       if (project_tmpInput_1 instanceof UnsafeArrayData) {
/* 072 */         rdd_mutableStateArray_0[1].write((UnsafeArrayData) project_tmpInput_1);
/* 073 */       } else {
/* 074 */         final int project_numElements_1 = project_tmpInput_1.numElements();
/* 075 */         rdd_mutableStateArray_1[3].initialize(project_numElements_1);
/* 076 */
/* 077 */         for (int project_index_1 = 0; project_index_1 < project_numElements_1; project_index_1++) {
/* 078 */           rdd_mutableStateArray_1[3].write(project_index_1, project_tmpInput_1.getDouble(project_index_1));
/* 079 */         }
/* 080 */       }
/* 081 */
/* 082 */       rdd_mutableStateArray_0[1].setOffsetAndSizeFromPreviousCursor(2, project_previousCursor_1);
/* 083 */
/* 084 */       // Remember the current cursor so that we can calculate how many bytes are
/* 085 */       // written later.
/* 086 */       final int project_previousCursor_2 = rdd_mutableStateArray_0[1].cursor();
/* 087 */
/* 088 */       final ArrayData project_tmpInput_2 = project_value_3;
/* 089 */       if (project_tmpInput_2 instanceof UnsafeArrayData) {
/* 090 */         rdd_mutableStateArray_0[1].write((UnsafeArrayData) project_tmpInput_2);
/* 091 */       } else {
/* 092 */         final int project_numElements_2 = project_tmpInput_2.numElements();
/* 093 */         rdd_mutableStateArray_1[4].initialize(project_numElements_2);
/* 094 */
/* 095 */         for (int project_index_2 = 0; project_index_2 < project_numElements_2; project_index_2++) {
/* 096 */           rdd_mutableStateArray_1[4].write(project_index_2, project_tmpInput_2.getDouble(project_index_2));
/* 097 */         }
/* 098 */       }
/* 099 */
/* 100 */       rdd_mutableStateArray_0[1].setOffsetAndSizeFromPreviousCursor(3, project_previousCursor_2);
/* 101 */       append((rdd_mutableStateArray_0[1].getRow()));
/* 102 */       if (shouldStop()) return;
/* 103 */     }
/* 104 */   }
/* 105 */
/* 106 */ }
```
程序输出效果如下：
```text
+---+----------+------------+----------------------+
|idx|    point1|      point2|my_foo(point1, point2)|
+---+----------+------------+----------------------+
|  1|(1.0, 1.0)|(10.0, 10.0)|          (11.0, 11.0)|
|  2|(2.0, 2.0)|(20.0, 20.0)|          (22.0, 22.0)|
|  3|(3.0, 3.0)|(30.0, 30.0)|          (33.0, 33.0)|
|  4|(4.0, 4.0)|(40.0, 40.0)|          (44.0, 44.0)|
|  5|(5.0, 5.0)|(50.0, 50.0)|          (55.0, 55.0)|
+---+----------+------------+----------------------+
```
