package com.study.spark.mlib

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
/**
  * Created by piguanghua on 2017/12/8.
  */
object TFIDFWeb {
	case class RawDataRecord(category: String, text: String)

	def main(args: Array[String]) {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		import spark.implicits._
		val sc = spark.sparkContext //创建环境变量实例
		//将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
		var srcDF = sc.textFile("D:\\Data\\lxw1234.txt").map {
			x =>
				var data = x.split(",")
				RawDataRecord(data(0),data(1))
		}.toDF()
		srcDF.select("category", "text").take(2).foreach(println)
		//将分好的词转换为数组
		var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
		var wordsData = tokenizer.transform(srcDF)
		wordsData.select($"category",$"text",$"words").take(2).foreach(println)

		//将每个词转换成Int型，并计算其在文档中的词频（TF）
		var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
		var featurizedData = hashingTF.transform(wordsData)
		featurizedData.foreach(println(_))

		//计算TF-IDF值
		var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
		var idfModel = idf.fit(featurizedData)
		var rescaledData = idfModel.transform(featurizedData)
		rescaledData.select($"category", $"words", $"features").take(2).foreach(println)

		var trainDataRdd = rescaledData.select($"category",$"features").map {
			case Row(label: String, features: Vector) =>
				LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
		}

		trainDataRdd.foreach(println(_))
	}
}
