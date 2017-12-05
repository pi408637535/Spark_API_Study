package com.study.spark.api

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/4.
  */
object Cartesian {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder()
		  .appName("AggregateFunction")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		val sc = spark.sparkContext

		val clothesRdd = sc.parallelize(Array("夹克", "T恤", "皮衣", "风衣"))
		val trousersRdd = sc.parallelize(Array("皮裤", "运动裤", "牛仔裤", "休闲裤"))

		clothesRdd.cartesian(trousersRdd).foreach(println(_))
	}
}
