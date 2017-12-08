package com.study.spark.mlib

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/7.
  */
object TestLabeledPoint2 {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sc = spark.sparkContext
		val mu = MLUtils.loadLibSVMFile(sc , "D:\\Data\\sample_kmeans_data.txt")
		mu.foreach(println(_))

	}
}
