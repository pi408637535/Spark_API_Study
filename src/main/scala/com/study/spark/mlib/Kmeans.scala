package com.study.spark.mlib

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Kmeans {
	def main(args: Array[String]) {

		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sc = spark.sparkContext //创建环境变量实例                            //创建环境变量实例
		val data =sc.textFile("D:\\Data\\kmeans_data.txt") //输入数据集
		val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
		val numClusters = 2
		val numIterations = 20
		val clusters = KMeans.train(parsedData, numClusters, numIterations)

		clusters.clusterCenters.foreach(println)

		println("<<<<<<<<<<<<<<<<<<<<<<<<<")

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		val WSSSE = clusters.computeCost(parsedData)
		println("Within Set Sum of Squared Errors = " + WSSSE)

		// Save and load model
	//	clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
	//	val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
		// $example off$

		sc.stop()

	}
}
