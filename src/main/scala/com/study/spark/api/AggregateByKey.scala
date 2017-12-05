package com.study.spark.api

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by piguanghua on 2017/12/4.
  */
object AggregateByKey {
	def main(args: Array[String]): Unit = {
			val spark = SparkSession
			  .builder()
			  .appName("AggregateFunction")
			  .master("local[*]")
			  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
			  .getOrCreate()
			val sc = spark.sparkContext

		val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
		val data = sc.parallelize(keysWithValuesList)
		//Create key value pairs
		val kv = data.map(_.split("=")).map(v => (v(0), v(1)))

		val initialSet = mutable.HashSet.empty[String]
		val addToSet = (s: mutable.HashSet[String], v: String) => s += v
		val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

		val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
		uniqueByKey.foreach(println(_))
		println(">>>>>>>>")
	}
}
