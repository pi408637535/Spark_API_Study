package com.study.spark.api

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/4.
  */
object Union {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder()
		  .appName("AggregateFunction")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		val sc = spark.sparkContext
		val studentNames: List[String] = List("张三", "李四", "王二", "麻子", "小敏", "豆豆", "瓜瓜", "丹丹")
		val studentNames2: List[String] = List("东欧", "虫子", "蝴蝶", "花")
		val studentNamesRdd = sc.parallelize(studentNames, 2)
		val studentNamesRdd2 = sc.parallelize(studentNames2, 2)
		studentNamesRdd.union(studentNamesRdd2).foreach(println(_))

	}
}
