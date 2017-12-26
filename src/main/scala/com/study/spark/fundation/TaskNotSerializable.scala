package com.study.spark.fundation

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/19.
  */
object TaskNotSerializable {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()


	}
}
