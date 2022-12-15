package com.lysoft.flink.hotitemsAnalysis

import java.sql.Timestamp
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timestamp: Long)

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {

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

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("hotitems", new SimpleStringSchema(), props))

    //val filePath: String = HotItems.getClass.getResource("/UserBehavior.csv").getPath
    //val dataStream: DataStream[UserBehavior] = env.readTextFile(filePath).map(line => {
    val dataStream: DataStream[UserBehavior] = kafkaStream.map(line => {
      val arr: Array[String] = line.split("\\,")
      UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toLong, arr(3).trim, arr(4).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val processedStream: DataStream[String] = dataStream.filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    processedStream.print()
    env.execute("hot items job")

  }

}

//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = {
    println("add")
    accumulator + 1
  }

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b

}

//自定义预聚合函数，计算平均值
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = {
    (accumulator._1 + value.timestamp, accumulator._2 + 1)
  }

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)

}

//自定义窗口函数，输出ItemViewCount
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    println("apply")
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }

}

class TopNHotItems(topN: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    println("processElement")
    //把每一条数据都放入状态列表
    itemState.add(value)
    //注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    println("onTimer")
    //将state中所有数据放入一个ListBuffer
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }

    //按照count大小倒序排序，并取前N个
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    //将排名结果格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")
    //输出每一个商品的信息
    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append("商品Id=").append(currentItem.itemId)
        .append("浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("=======================")
    //控制输出频率
    TimeUnit.MILLISECONDS.sleep(1000)
    out.collect(result.toString())

    //清空状态
    itemState.clear()
  }

}
