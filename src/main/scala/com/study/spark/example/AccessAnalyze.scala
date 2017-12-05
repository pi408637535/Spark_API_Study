package com.study.spark.example

import org.apache.spark.sql.SparkSession

/**
  * 唯一的一个标识是你的deviceID
  * 上行流量指的是手机app向服务器发送的请求数据的流量
  * 下行流量，认为是服务器端给手机app返回的数据（比如说图片、文字、json）的流量
  * 每个设备（deviceID），总上行流量和总下行流量，计算之后，要根据上行流量和下行流量进行排序，需要进行倒序排序
  *   获取流量最大的前10个设备
  * Created by piguanghua on 2017/12/5.
  */
object AccessAnalyze {

	case class Device(deviceId: String, upload:Int, download:Int)

	implicit object DeviceOrdering extends Ordering[Device] {
		override def compare(e1:Device, e2:Device): Int = {
			(e1.upload-e2.upload)
		}
	}

	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder()
		  .appName("AggregateFunction")
		  .master("local[*]")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()
		val sc = spark.sparkContext


		val textRdd = sc.textFile("file:///D://Data//access.log")
		textRdd.map(line=>{
			val strings = line.split("\t")
			(strings(1),Device(strings(1), strings(2).toInt, strings(3).toInt))
		}).reduceByKey((oriDevice,otherDevice)=>{
			Device(oriDevice.deviceId, oriDevice.upload + otherDevice.upload, oriDevice.download+otherDevice.download)
		}).map(ele=>{
			((ele._2.download+ele._2.upload), ele._2)
		}).sortByKey().collect()
		  .map{case(total:Int,device: Device)=>{
			device
		}}.sortBy(device=>{
			device
		}).foreach(println(_))


	}
}
