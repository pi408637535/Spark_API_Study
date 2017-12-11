package com.study.spark.mlib

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TF_IDF {
  def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
        .getOrCreate()

      val sc = spark.sparkContext //创建环境变量实例

    val documents = sc.textFile("D:\\Data\\tf_idf.txt").map(_.split(" ").toSeq)		//读取数据文件

    val hashingTF = new HashingTF()							//首先创建TF计算实例
    val tf = hashingTF.transform(documents)			//计算文档TF值
    val idf = new IDF().fit(tf)									//创建IDF实例并计算

    val tf_idf= idf.transform(tf)									//计算TF_IDF词频
    tf_idf.foreach(println)										//打印结果

  }
}
