package com.study.spark.fundation

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/19.
  */
class MyTest2 (conf:String)  extends Serializable {
	val list = List("a.com", "www.b.com", "a.cn", "a.com.cn", "a.org");
	private val spark = SparkSession
	  .builder
	  .appName(s"${this.getClass.getSimpleName}")
	  .master("local[*]")
	  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
	  .getOrCreate()
	private val sc = spark.sparkContext
	val rdd = sc.parallelize(list);

	/*def getResult(): Array[(String)] = {
		val rootDomain = conf
		val result = rdd.filter(item => item.contains(rootDomain))
		  .map(item => addWWW(item))
		result.take(result.count().toInt)
	}*/
	/*def addWWW(str:String):String = {
		if(str.startsWith("www."))
			str
		else
			"www."+str
	}*/

	def getResult(): Array[(String)] = {
		val rootDomain = conf
		val result = rdd.filter(item => item.contains(rootDomain))
		  .map(item => UtilTool.addWWW(item))
		result.take(result.count().toInt)
	}

	object UtilTool {
		def addWWW(str:String):String = {
			if(str.startsWith("www."))
				str
			else
				"www."+str
		}
	}

	/*val addWWW = (str:String) => {
		if(str.startsWith("www."))
			str
		else
			"www."+str
	}*/
}

object MyTest2{
	def main(args: Array[String]): Unit = {
		val myTest2 = new MyTest2("a.org")
		myTest2.getResult().foreach(println)
	}
}
