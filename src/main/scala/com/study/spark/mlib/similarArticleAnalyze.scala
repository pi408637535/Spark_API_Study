package com.study.spark.mlib

import org.apache.spark.sql.SparkSession
import breeze.linalg.{SparseVector, norm}
import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
/**
  * 利用TF-IDF + 余弦公式
  * 文章格式： typeId====文章ID====标题====正文
  * Created by piguanghua on 2017/12/11.
  */
object similarArticleAnalyze {


	def  splitWordToSeq(news:String)={
		val terms:java.util.List[Term] =ToAnalysis.parse(news).getTerms
		val size=terms.size()
		var res=""
		for( i<- 0 until size){
			res+=terms.get(i.toInt).getName+" "
		}
		res.split(" ")
	}

	def main(args: Array[String]) {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		import spark.implicits._
		val sc = spark.sparkContext //创建环境变量实例

		//加载数据RDD格式 (id,contnent,typeId,title)
		val src = sc.textFile("").map(x => x.split("====").toSeq).filter(_.length==4);
		//（文章ID,[词1,词2,.],分类ID,标题)
		val trainRdd= src.map(x => (x(1),splitWordToSeq(x(3)),x(0),x(2)))

		//tf-idf
		val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
		//计算TF
		val newSTF = trainRdd.map {
			case (num, seq,typename,title) =>
				val tf = hashingTF.transform(seq)
				(num, tf)
		}
		newSTF.cache()
		newSTF.take(10).foreach(println)
		//构建idf model
		val idf = new IDF().fit(newSTF.values)
		//将tf向量转换成tf-idf向量
		val newsIDF = newSTF.mapValues(v => idf.transform(v))


		//广播一份tf-idf向量集
		val bIDF = sc.broadcast(newsIDF.collect())

		val docSims = newsIDF.flatMap {
			case (id1, idf1) =>
				val idfs = bIDF.value.filter(_._1 != id1)
				val sv1 = idf1.asInstanceOf[SV]
				//构建向量1
				val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
				//取相似度最大的前10个
				idfs.map {
					case (id2, idf2) =>
						val sv2 = idf2.asInstanceOf[SV]
						//构建向量2
						val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
						//计算两向量点乘除以两向量范数得到向量余弦值
						val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))
						(id1, id2, cosSim)
				}.sortWith(_._3 > _._3).take(10)
		}

		//每篇文章相似并排序，取最相似的前10个
		val simRdd= docSims.groupBy(x=>x._1)
		val srcJoin=src.map(x=>(x(1),x(2)))

		//广播一份srcJoin
		val bSrcJoin = sc.broadcast(srcJoin.collect())

		//按标题输出
		srcJoin.join(simRdd).map(x=>(x._1,x._2._1,x._2._2.map(x=>(x._2,x._3)).map(x=>{
			val id=x._1
			val sim=x._2
			val  name=bSrcJoin.value.filter(x=>x._1==id).take(1).toList.mkString(",")
			name+" "+sim
		}
		))).foreach(x=>println(x._1+" "+x._2+" "+x._3))


		spark.stop()
	}
}
