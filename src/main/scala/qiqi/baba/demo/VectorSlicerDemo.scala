package qiqi.baba.demo


/**
  * Created by lhd on 8/8/17.
  *
  * VectorSlicer是一个转换器，以特征向量和与原特征的子阵列的一种新的特征向量，输出。
  * 它对于从向量列中提取特征非常有用。vectorslicer接受指定指标向量列，然后输出一个新的向量列的值是通过这些指标的选择。
  * 有两类指数，整数指数代表的指标为载体，setindices()。字符串的下标表示功能的名称为载体，setnames()。
  * 这就要求矢量列有一个属性组实施以来比赛的一个属性的名称字段。整数和字符串的规范都是可以接受的。
  * 此外，还可以同时使用整数索引和字符串名称。必须选择至少一个特性。
  * 不允许重复的特性，因此在选定的索引和名称之间不会有重叠。
  * 注意，如果选择了名称的名称，如果遇到空的输入属性，就会引发异常。
  * 输出向量将首先选择所选索引的特性（按给定的顺序），然后选择所选的名称（按给定的顺序）。
  */
object VectorSlicerDemo extends App with BasicClass{
//  import org.apache.spark.ml.linalg.Vectors
//  import org.apache.spark.ml.feature.ChiSqSelector
//  val data = Seq(
//    (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
//    (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
//    (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
//  )
//
//  //val beanRDD = sc.parallelize(data).map(t3 => Bean(t3._1,t3._2,t3._3))
//  //spark.sparkContext.parallelize(data).map(t=>Bean(t._1,t._2,t._3))
//
//  val df = spark.createDataFrame(data).toDF("id","features","clicked")
//
//  val selector = new ChiSqSelector()
//    .setNumTopFeatures(2)
//    .setFeaturesCol("features")
//    .setLabelCol("clicked")
//    .setOutputCol("selectedFeatures")
//
//  val result = selector.fit(df).transform(df)
//  result.show(false)
  import java.util.Arrays

  import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
  import org.apache.spark.ml.feature.VectorSlicer
  import org.apache.spark.ml.linalg.Vectors
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.StructType

  val data = Arrays.asList(Row(Vectors.dense(-2.0, 2.3, 0.0)))

  val defaultAttr = NumericAttribute.defaultAttr
  val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
  val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

  val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

  val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

  slicer.setIndices(Array(1)).setNames(Array("f3"))
  // or slicer.setIndices(Array(1, 2)),
  // or slicer.setNames(Array("f2", "f3"))

  val output = slicer.transform(dataset)
  output.select("userFeatures", "features").show(false)
}
