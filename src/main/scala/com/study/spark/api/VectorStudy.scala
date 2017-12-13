package com.study.spark.api

import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/13.
  */
object VectorStudy {

	def  splitWordToSeq(news:String)={
		val terms:java.util.List[Term] =ToAnalysis.parse(news).getTerms
		val size=terms.size()
		var res=""
		for( i<- 0 until size){
			res+=terms.get(i.toInt).getName+" "
		}
		res.split(" ")
	}

	def main(args : Array[String]) {

		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		import spark.implicits._
		val sc = spark.sparkContext //创建环境变量实例

		val stringData = "我从放假哦第三空间发大水了看法简单实例会计分录卡萨范德萨发大水看风景了看法"
		val data = splitWordToSeq(stringData)

		val hashingTf = new HashingTF()
		val tfs = hashingTf.transform(data)
		val rddVector = sc.parallelize(Seq(tfs))

		val idf = new IDF().fit(rddVector)





		//val ide = new IDF().fit(tf)
	}


}
