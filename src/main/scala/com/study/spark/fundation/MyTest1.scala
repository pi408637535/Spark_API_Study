package com.study.spark.fundation

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/19.
  * 案例1.map,filter etc算子，内部引用了类成员函数或变量导致该类所有成员都需要支持序列化，又由于某些类成员不知道序列化，导致该问题
  */
class MyTest1(conf:String) extends Serializable{
	val list = List("a.com", "www.b.com", "a.cn", "a.com.cn", "a.org");
	private val spark = SparkSession
	  .builder
	  .appName(s"${this.getClass.getSimpleName}")
	  .master("local[*]")
	  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
	  .getOrCreate()
	@transient  private val sc = spark.sparkContext
	 val rdd = sc.parallelize(list);

	private val rootDomain = conf

	def getResult(): Array[(String)] = {
		val result = rdd.filter(item => item.contains(rootDomain))
		result.take(result.count().toInt)
	}
}

object MyTest1{
	def main(args: Array[String]): Unit = {
		val myTest1 = new MyTest1("a.org")
		myTest1.getResult().foreach(println)
	}
}
