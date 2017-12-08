package com.study.spark.mlib

import org.apache.spark.mllib.linalg.Matrices

/**
  * Created by piguanghua on 2017/12/7.
  */
object TestMatrix extends App {
	val mx = Matrices.dense(2, 3,Array(1,2,3,4,5,6) )
	println(mx)
}
