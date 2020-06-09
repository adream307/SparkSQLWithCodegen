# 自定义函数示例2

在[上一篇](./udf_example1.md)文章中，我们介绍了使用 Spark 原生的 UDF 实现的自定义函数，在这里我们介绍一种非 UDF 实现的自定义函数，为后面介绍的 Codegen 实现自定义函数打基础。


## 非 UDF 的自定义函数
自定义函数的需求与前文一样：

- 需要一个名字为 `my_foo` 的函数
- 该函数接受两个 `double` 类型的参数作为输入
- 参数名记为 `x，y`
- 函数输出 `x*y+3` 

非 UDF 自定义函数代码如下:

```scala
package org.apache.spark.sql.myfunctions {

  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
  import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
  import org.apache.spark.sql.types.{AbstractDataType, DoubleType}

  case class my_foo(inputExpressions: Seq[Expression]) extends Expression with ExpectsInputTypes with CodegenFallback {

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = {
      val left = inputExpressions(0).eval(input)
      val right = inputExpressions(1).eval(input)
      if (left != null && right != null) {
        left.asInstanceOf[Double] * right.asInstanceOf[Double] + 3
      }
      else {
        null
      }
    }

    override def dataType: DataType = DoubleType

    override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

    override def children: Seq[Expression] = inputExpressions
  }

}
```

## 程序说明
1. `myfunctions` 这个 package 必须在 `org.apache.spark.sql` 名字空间下
2. `my_foo` 需要集成 `Expression`
3. `ExpectsInputTypes` 和 `CodegenFallback` 属于 `trait`，属于额外约束项目
4. `ExpectsInputTypes` 确保输入类型必须为 `Double`
5. `CodegenFallback` 确保不产生 `Codegen` 代码，程序之心路径进入 `eval` 函数
6. `my_foo` 的具体实现在 `evla` 函数内
7. `eval` 首先判断输入数据不为 `null`，然后返回 `x*y+3` 否则返回 `null`


## 注册并运行程序
与 UDF 类似， `case class` 实现的自定义函数也需要注册，注册代码如下:
```scala
import org.apache.spark.sql.myfunctions.my_foo

spark.sessionState.functionRegistry.createOrReplaceTempFunction("my_foo", my_foo)
```

完整代码位于 `code/udf_example2` 目录内，程序运行效果如下：

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

输出执行计划树如下：

```text
== Physical Plan ==
Project [x#2, y#3, my_foo(x#2, y#3) AS my_foo(x, y)#6]
+- *(1) Scan ExistingRDD[x#2,y#3]
```