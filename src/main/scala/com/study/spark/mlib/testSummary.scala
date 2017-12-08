package com.study.spark.mlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

object testSummary {
	def main(args: Array[String]) {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sc = spark.sparkContext //创建环境变量实例
		val rdd = sc.textFile("D:\\Data\\testSummary.txt") //创建RDD文件路径
		  .map(_.split(' ') //按“ ”分割
		  .map(_.toDouble)) //转成Double类型
		  .map(line => Vectors.dense(line)) //转成Vector格式
		val summary = Statistics.colStats(rdd) //获取Statistics实例
		println(summary.mean) //计算均值
		println(summary.variance) //计算标准差
	}
}
