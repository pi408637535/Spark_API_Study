package com.study.spark.fundation

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/12/19.
  */
object Test3 {
	def main(args : Array[String]) {
		val spark = SparkSession
		  .builder
		  .appName(s"${this.getClass.getSimpleName}")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		val sc = spark.sparkContext
		val matrix = new DenseMatrix(2, 2, Array(1.0, 2, 3, 4))
		new Test(sc, matrix).run()
	}
}

class Test(@transient scc:SparkContext,PHI:DenseMatrix) extends Serializable{
	val ts = 0.1
	def run(): Unit ={
		val rdds = scc.parallelize(0 to 3)
		val a = rdds.map(
			x =>{
				PHI.toArray.apply(x)*x
			}
		)
		a.collect.foreach(println(_))
	}
}

