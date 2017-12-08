package com.study.spark.mlib

import org.apache.spark.sql.SparkSession

object testStratifiedSampling2 {
	def main(args: Array[String]) {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sc = spark.sparkContext //创建环境变量实例
		val data = sc.textFile("D:\\Data\\mlib_sampling.txt") //读取数据
		  .map(row => { //开始处理
			if (row.length == 3) //判断字符数
				(row, 1) //建立对应map
			else (row, 2) //建立对应map
		}).map(each => (each._2, each._1))
		val fractions: Map[Int, Double] = (List((1, 0.2), (2, 0.8))).toMap
	//	val fractions: Map[String, Double] = Map("aa" -> 2) //设定抽样格式
		val approxSample = data.sampleByKey(withReplacement = false, fractions, 0) //计算抽样样本
		approxSample.foreach(println) //打印结果
	}
}
