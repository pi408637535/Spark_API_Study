package com.study.spark.api

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/4.
  * 对partition下所有数据,全部使用map  速度快，数据量限制在1000mislion
  */
object MapPartitions {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder()
		  .appName("AggregateFunction")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		val sc = spark.sparkContext

		val studentNames: List[String] = List("张三", "李四", "王二", "麻子")
			val studentNamesRdd = sc.parallelize(studentNames, 2)
		//val studentNamesRdd = sc.makeRDD(studentNames, 1)
		val studentScoreMap: Map[String, Double] = Map("张三" -> 278.5, "李四" -> 290.0, "王二" -> 301.0, "麻子" -> 205)

		studentNamesRdd.foreach(println(_))

		var rdd3 = studentNamesRdd.mapPartitions {
			ele => {
				var studentScoreList:List[Double] = List[Double]()
				while (ele.hasNext) {
					val studentName = ele.next()
					val studentScore = studentScoreMap.get(studentName)
					studentScoreList = studentScoreList.::(studentScore.get)
				}
				studentScoreList.iterator
			}
		}

		/*var rdd1 = sc.makeRDD(1 to 5, 2)
		//rdd1有两个分区
		var rdd3 = rdd1.mapPartitions { x => {
			var result = List[Double]()
			var i = 0
			while (x.hasNext) {
				i += x.next()
			}
			result.::(i).iterator
			}
		}*/

		rdd3.foreach(println(_))

	}
}
