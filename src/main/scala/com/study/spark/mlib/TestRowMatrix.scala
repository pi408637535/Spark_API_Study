package com.study.spark.mlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/7.
  */
object TestRowMatrix {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sc = spark.sparkContext
		val rdd = sc.textFile("D:\\Data\\rowMatrix.txt")

		val rowMaxtrix  = rdd.map(_.split(" ").map(_.toDouble)) .map(line => Vectors.dense(line))

		val rm = new RowMatrix(rowMaxtrix)
		println(rm.numRows())
		println(rm.numCols())


	}
}
