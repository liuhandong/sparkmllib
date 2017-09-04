package qiqi.baba.demo

/**
  * Created by lhd on 8/17/17.
  * http://blog.csdn.net/illbehere/article/details/54378352
  * 逻辑回归
  */
object LogisticRegressionDemo extends App with BasicClass{
  import org.apache.spark.ml.classification.LogisticRegression

  // Load training data
  val training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  // Fit the model
  val lrModel = lr.fit(training)

  // Print the coefficients and intercept for logistic regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  // We can also use the multinomial family for binary classification
  val mlr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    .setFamily("multinomial")

  val mlrModel = mlr.fit(training)

  // Print the coefficients and intercepts for logistic regression with multinomial family
  println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
  println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
}
