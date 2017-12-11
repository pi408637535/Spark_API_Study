package com.study.spark.mlib

import com.study.spark.mlib.similarArticleAnalyze.splitWordToSeq
import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.regression.LabeledPoint
/**
  * 文章格式: typeId,正文
  * Created by piguanghua on 2017/12/11.
  */
object TestNaiveBayes {
	case class RawDataRecord(category: String, text: String)

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

		val src = sc.textFile("D:\\Data\\native_bayes.txt").map(x => x.split(",").toSeq)
		val trainRdd= src.map(x => (x(0).toDouble,splitWordToSeq(x(1))))
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

		val dataRdd = newsIDF.map(ele=>{
			LabeledPoint(ele._1, ele._2)
		})

		val Array(training, test) = dataRdd.randomSplit(Array(0.6, 0.4))
		val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

	/*	val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
		val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
		println(accuracy)*/


		val res = sc.textFile("D:\\Data\\native_demo.txt").map(x => x.split(",").toSeq)
		val trainRdd1= res.map(x => (x(0).toDouble,splitWordToSeq(x(1))))
		trainRdd1.foreach(x=>println( "trainRdd1:" + x ))
		//计算TF
		val newSTF1 = trainRdd1.map {
			case (num, seq) =>
				val tf = hashingTF.transform(seq)
				(num, tf)
		}

		val idfs1 = new IDF().fit(newSTF1.values)
		val newsIDF1 = newSTF1.mapValues(v => idfs1.transform(v))
		val dataRdd1 = newsIDF1.map(ele=>{
			LabeledPoint(ele._1, ele._2)
		})
		val predictionAndLabel = dataRdd1.map(p => {
			println("before----"+p.label)
			println("predict" + model.predict(p.features))
			(model.predict(p.features), p.label)
		})
		predictionAndLabel.take(0).foreach(ele=>println("result："+ele._1))
		println(predictionAndLabel.foreach(x=>">>>>><<<<<<<<<<<<" + x._2))


/*
		var srcRDD = sc.textFile("/tmp/lxw1234/sougou/").map {
			x =>
				var data = x.split(",")
				RawDataRecord(data(0),data(1))
		}

		//70%作为训练数据，30%作为测试数据
		val splits = srcRDD.randomSplit(Array(0.7, 0.3))
		val trainingDF = splits(0).toDF()
		val testDF = splits(1).toDF()

		//将词语转换成数组
		val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
		val wordsData = tokenizer.transform(trainingDF)
		println("output1：")
		wordsData.select($"category",$"text",$"words").take(1)

		//计算每个词在文档中的词频
		var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
		var featurizedData = hashingTF.transform(wordsData)
		println("output2：")
		featurizedData.select($"category", $"words", $"rawFeatures").take(1)


		//计算每个词的TF-IDF
		var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
		var idfModel = idf.fit(featurizedData)
		var rescaledData = idfModel.transform(featurizedData)
		println("output3：")
		rescaledData.select($"category", $"features").take(1)

		//转换成Bayes的输入格式
		var trainDataRdd = rescaledData.select($"category",$"features").map {
			case Row(label: String, features: Vector) =>
				LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
		}
		println("output4：")
		trainDataRdd.take(1)*/

		//训练模型
		/*val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

		//测试数据集，做同样的特征表示及格式转换
		var testwordsData = tokenizer.transform(testDF)
		var testfeaturizedData = hashingTF.transform(testwordsData)
		var testrescaledData = idfModel.transform(testfeaturizedData)
		var testDataRdd = testrescaledData.select($"category",$"features").map {
			case Row(label: String, features: Vector) =>
				LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
		}

		//对测试数据集使用训练模型进行分类预测
		val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

		//统计分类准确率
		var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
		println("output5：")
		println(testaccuracy)*/

	}
}
