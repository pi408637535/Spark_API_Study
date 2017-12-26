package com.study.spark.mlib

import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/13.
  */
object TestNaiveBayesLocal {
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
		val src = sc.textFile("hdfs://192.168.152.138:9000/ml/text/span_forum2.txt").map(x => x.split("==").toSeq)
		val trainRdd = src
		  .map(
			  x => {
				  (x(0).toDouble, splitWordToSeq(x(1)))
			  }
		  )
		//tf-idf
		val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
		//计算TF
		val newSTF = trainRdd.map {
			case (num, seq) =>
				val tf = hashingTF.transform(seq)
				(num, tf)
		}

		val idfs = new IDF().fit(newSTF.values)
		val newsIDF = newSTF.mapValues(v => idfs.transform(v))

		val dataRdd = newsIDF.map(ele => {
			LabeledPoint(ele._1, ele._2)
		})

		val Array(training, test) = dataRdd.randomSplit(Array(0.8, 0.2))
		val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
		val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
		val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
		println(accuracy)

		val stringData = "习近平饭卡积分了肯定是减肥了看电视接发大水了看法"
		val data = splitWordToSeq(stringData)


		val tfs = hashingTF.transform(data)


		val rddVector = sc.parallelize(Seq(tfs))
		val idf = new IDF().fit(rddVector)

		val tf_idf= idf.transform(tfs)
		println( "result >>>>" +  model.predict(tf_idf) )
	}
}
