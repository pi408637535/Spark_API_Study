package com.study.spark.api

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/4.
  * mapPartitionsWithIndex 拿到每个partitions的索引，对同一个partition数据进行操作。
  */
object MapPartitionsWithIndex {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder()
		  .appName("AggregateFunction")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		val sc = spark.sparkContext

		val studentNames: List[String] = List("张三", "李四", "王二", "麻子")
		val studentNamesRdd = sc.parallelize(studentNames, 3)

		val rdd2 = studentNamesRdd.mapPartitionsWithIndex((index,ele)=>{
			var studentWithClassList:List[String] = List[String]()

			while (ele.hasNext){
				val studentName = ele.next()
				val studentWithClass = studentName + "_" + (index + 1)
				studentWithClassList = studentWithClassList.::(studentWithClass)
			}
			studentWithClassList.iterator
		})

		rdd2.foreach(println(_))

		/*var rdd1 = sc.makeRDD(1 to 5,2)
		//rdd1有两个分区
		var rdd2 = rdd1.mapPartitionsWithIndex{
			(x,iter) => {
				var result = List[String]()
				var i = 0
				while(iter.hasNext){
					i += iter.next()
				}
				result.::(x + "|" + i).iterator

			}
		}
		rdd2.foreach(println(_))*/

	}
}
