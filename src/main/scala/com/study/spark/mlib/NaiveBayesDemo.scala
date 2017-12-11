package com.study.spark.mlib

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/11.
  */
object NaiveBayesDemo {
	def main(args: Array[String]) {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		import spark.implicits._
		val sc = spark.sparkContext //创建环境变量实例
		//加载数据
		val path = "/tmp/NaiveBayesdata.txt"
		val tf = new HashingTF(numFeatures = 100)

		val lines = sc.textFile(path)
		lines.map(string=>{
			val sarry = string.split(",")
			val dLabel = sarry(0).toDouble
			val sFeatures = sarry(1).split(" ")
			val spamFeatures = tf.transform(sFeatures)
	//		val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))

		})
	}
}
