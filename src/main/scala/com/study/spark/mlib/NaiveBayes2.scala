package com.study.spark.mlib

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/11.
  */
object NaiveBayes2 {
	def main(args: Array[String]) {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		import spark.implicits._
		val sc = spark.sparkContext //创建环境变量实例
		val data_path ="D:\\Data\\sample_naive_bayes_data.txt"
		//读取数据，转换成LabeledPoint类型
		val examples =sc.textFile(data_path).map { line =>
			val items =line.split(',')
			val label =items(0).toDouble
			val value =items(1).split(' ').toArray.map(f =>f.toDouble)
			LabeledPoint(label, Vectors.dense(value))
		}
		//样本划分，80%训练，20%测试
		val splits =examples.randomSplit(Array(0.8,0.2))
		val training =splits(0)
		val test =splits(1)
		val numTraining =training.count()
		val numTest =test.count()
		println(s"numTraining = $numTraining, numTest = $numTest.")
		//样本训练，生成分类模型
		val model =new NaiveBayes().setLambda(1.0).run(training)
		//根据分类模型，对测试数据进行测试，计算测试数据的正常率
		val prediction =model.predict(test.map(_.features))
		val predictionAndLabel =prediction.zip(test.map(_.label))

		predictionAndLabel.foreach(println(_))

		val accuracy =predictionAndLabel.filter(x => x._1 == x._2).count().toDouble / numTest
		println(s"Test accuracy = $accuracy.")
	}
}
