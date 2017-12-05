package com.study.spark.api

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/5.
  */
object SortBy {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder()
		  .appName("AggregateFunction")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		val sc = spark.sparkContext
		val array = Array((1, 6, 3), (2, 3, 3), (1, 1, 2), (1, 3, 5), (2, 1, 2))
		val rdd1 = sc.parallelize(array)
		//设置元素(e1,e3)为key,value为原来的整体
		val rdd2 = rdd1.map(f => ((f._1, f._3), f))
		rdd2.foreach(println(_))
		println("<<<<<<<<<<<<<<<<<")
		//利用sortByKey排序的对key的特性
		val rdd3 = rdd2.sortByKey()
		rdd3.values.collect.foreach(println(_))
	}
}
