package com.lysoft.flink.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义事件样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//定义窗口预聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置全局并行度
    env.setParallelism(1)

    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //从kafka读取数据
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers", "172.18.46.204:9092")
    props.setProperty("group.id", "CID_alikafka_hsrj_flume")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    //val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("", new SimpleStringSchema(), props))
    val dataStream = env.readTextFile("D:\\ideaworkspace\\study\\bigdata\\flink1.7\\src\\main\\resources\\apache.log")
    val processedStream = dataStream.map(data => {
      val dataArray = data.split(" ")
      // 定义时间转换
      val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
      ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = {
        element.eventTime
      }
    })
    .keyBy(_.url)
    .timeWindow(Time.minutes(10), Time.seconds(5))
    .allowedLateness(Time.seconds(60))
    .aggregate(new CountAgg(), new WindowResult())
    .keyBy(_.windowEnd)
    .process(new TopNHotUrls(5))

    dataStream.print("aggregate")
    processedStream.print("process")

    env.execute("network flow job")

  }

}

//自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口处理函数
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//自定义排序输出处理函数
class TopNHotUrls(topN: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    //把每一条数据都放入状态列表
    urlState.add(value)
    //注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将state中所有数据放入一个ListBuffer
    val allUrls: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]
    import scala.collection.JavaConversions._
    for (url <- urlState.get()) {
      allUrls += url
    }

    //按照count大小倒序排序，并取前N个
    val sortedUrls: ListBuffer[UrlViewCount] = allUrls.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    //将排名结果格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")
    //输出每一个商品的信息
    for (i <- sortedUrls.indices) {
      val currentUrl = sortedUrls(i)
      result.append("No").append(i + 1).append(":")
        .append("URL=").append(currentUrl.url)
        .append("浏览量=").append(currentUrl.count)
        .append("\n")
    }
    result.append("=======================")
    //控制输出频率
    TimeUnit.MILLISECONDS.sleep(1000)
    out.collect(result.toString())

    //清空状态
    urlState.clear()
  }
}
