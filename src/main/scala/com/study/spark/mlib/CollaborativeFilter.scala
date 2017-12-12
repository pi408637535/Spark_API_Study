package com.study.spark.mlib

import org.apache.spark._
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

object CollaborativeFilter {
	def main(args: Array[String]) {

		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sc = spark.sparkContext //创建环境变量实例    								//设置数据集
		val data = sc.textFile("D:\\Data\\als.txt")
		val ratings = data.map(_.split(' ') match { //处理数据
			case Array(user, item, rate) => //将数据集转化
				Rating(user.toInt, item.toInt, rate.toDouble) //将数据集转化为专用Rating
		})
		val rank = 2 //设置隐藏因子
		val numIterations = 2 //设置迭代次数
		val model = ALS.train(ratings, rank, numIterations, 0.01) //进行模型训练
		var rs = model.recommendProducts(2, 1) //为用户2推荐一个商品
		rs.foreach(println) //打印结果
	}
}
