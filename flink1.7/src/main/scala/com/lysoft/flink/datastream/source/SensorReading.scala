package com.lysoft.flink.datastream.source

//传感器温度样例类
case class SensorReading(
  //传感器Id
  var id: String,
  //事件时间
  var timestamp: Long,
  //传感器温度
  var temperature: Double
)
