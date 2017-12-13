package com.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Created by piguanghua on 2017/12/12.
  */
object NetworkNaiveBayes {
	def main(args: Array[String]) {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .config("spark.sql.warehouse.dir", "some-value")
		  .getOrCreate()
		import spark.implicits._
		val sc = spark.sparkContext //创建环境变量实例
		val ssc = new StreamingContext(sc, Seconds(2))
		val lines = ssc.socketTextStream( "127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
		val words = lines.flatMap(_.split(" "))
		val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
		wordCounts.print()

		ssc.start()
		ssc.awaitTermination()
	}
}
