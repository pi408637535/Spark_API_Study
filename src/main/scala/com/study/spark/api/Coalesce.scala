package com.study.spark.api

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by piguanghua on 2017/12/4.
  * 将RDD的partition的数量压缩到更少的partition中去。
  * 建议与filter连用
  */
object Coalesce {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder()
		  .appName("AggregateFunction")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		val sc = spark.sparkContext

		val staffList = Array("张三", "李四", "王二", "麻子",
			"赵六", "王五", "李大个", "王大妞", "小明", "小倩", "消息","1","2", "3", "4", "5","6","7","8","9")
		val staffRdd = sc.parallelize(staffList, 6)

		val stafRdd2 = staffRdd.mapPartitionsWithIndex( (index, ele)=> {
			var stringList:List[String] = List[String]()

			while(ele.hasNext){
				stringList = stringList.::("partition:" +index +">>" +    ele.next() )
			}

			stringList.iterator
		})

		stafRdd2.foreach(println(_))

		println("》》》》》》》》》》》》》》》》》》》》》》")

		stafRdd2.coalesce(3).foreach(println(_))

	}
}
