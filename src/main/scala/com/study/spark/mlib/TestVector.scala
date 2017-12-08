package com.study.spark.mlib


import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by piguanghua on 2017/12/7.
  */
object TestVector {
	def main(args: Array[String]): Unit = {
		/*val vd = Vectors.dense(2, 0, 6)
		println(vd(2))*/
		val vs = Vectors.sparse(4, Array(0, 2,1,3), Array(9,5,2,7))
		println(vs(1))
	}
}
