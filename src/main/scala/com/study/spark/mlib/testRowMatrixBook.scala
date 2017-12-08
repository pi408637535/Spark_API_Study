package com.study.spark.mlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/7.
  */
object testRowMatrixBook {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sc = spark.sparkContext

		val rdd = sc.textFile("c://a.txt")                                     //创建RDD文件路径
		  .map(_.split(' ')                                                //按“ ”分割
		  .map(_.toDouble))                                             //转成Double类型
		  .map(line => Vectors.dense(line))                                //转成Vector格式
		val rm = new RowMatrix(rdd)                                      //读入行矩阵
		println(rm.numRows())                                           //打印列数
		println(rm.numCols())
	}
}
