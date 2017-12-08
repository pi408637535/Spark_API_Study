package com.study.spark.mlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by piguanghua on 2017/12/7.
  */
object TestLabeledPoint {
	def main(args: Array[String]): Unit = {
		val vd = Vectors.dense(1,2,3,4)
		val pos = LabeledPoint(1, vd)
		println(pos.features)
		println(pos.label)

	}
}
