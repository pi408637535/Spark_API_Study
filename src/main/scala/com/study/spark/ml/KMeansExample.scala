package com.study.spark.ml

import org.apache.spark.ml.clustering.KMeans
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/7.
  */
object KMeansExample {
def main(args: Array[String]): Unit = {
	val spark = SparkSession
	  .builder
	  .appName(s"${this.getClass.getSimpleName}")
	  .master("local[*]")
	  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
	  .getOrCreate()

	// $example on$
	// Loads data.
	val dataset = spark.read.format("libsvm").load("D:\\Data\\sample_kmeans_data.txt")

	// Trains a k-means model.
	val kmeans = new KMeans().setK(2).setSeed(1L)
	val model = kmeans.fit(dataset)

	// Evaluate clustering by computing Within Set Sum of Squared Errors.
	val WSSSE = model.computeCost(dataset)
	println(s"Within Set Sum of Squared Errors = $WSSSE")

	// Shows the result.
	println("Cluster Centers: ")
	model.clusterCenters.foreach(println)
	// $example off$

	spark.stop()
  }
}
