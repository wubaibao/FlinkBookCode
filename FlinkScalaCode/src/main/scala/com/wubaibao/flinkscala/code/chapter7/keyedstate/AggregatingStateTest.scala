package com.wubaibao.flinkscala.code.chapter7.keyedstate

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink AggregatingState 状态编程
 * 案例：读取基站日志数据，统计每个主叫号码通话平均时长。
 */
object AggregatingStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * Socket中数据如下:
     * 001,186,187,busy,1000,10
     * 002,187,186,fail,2000,20
     * 003,186,188,busy,3000,30
     * 004,187,186,busy,4000,40
     * 005,189,187,busy,5000,50
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    //对ds进行转换处理，得到StationLog对象
    val stationLogDS: DataStream[StationLog] = ds.map(new MapFunction[String, StationLog] {
      override def map(line: String): StationLog = {
        val arr: Array[String] = line.split(",")
        StationLog(
          arr(0).trim,
          arr(1).trim,
          arr(2).trim,
          arr(3).trim,
          arr(4).toLong,
          arr(5).toLong
        )
      }
    })

    stationLogDS.keyBy(stationLog => stationLog.callOut)
      .map(new RichMapFunction[StationLog, String] {
        //定义一个AggregatingState 用来存放同一个主叫号码的所有通话次数和通话时长
        private var aggregatingState: AggregatingState[StationLog, Double] = _


        override def open(parameters: Configuration): Unit = {
          //定义状态描述器
          val aggregatingStateDescriptor = new AggregatingStateDescriptor[StationLog, Tuple2[Long, Long], Double]("aggregatingStateDescriptor",
            new AggregateFunction[StationLog, Tuple2[Long, Long], Double] {
              //创建累加器，第一个位置存放通话次数，第二个位置存放总通话时长
              override def createAccumulator(): (Long, Long) = (0L, 0L)

              //每来一条数据，调用一次add方法
              override def add(value: StationLog, accumulator: (Long, Long)): (Long, Long) = (accumulator._1 + 1, accumulator._2 + value.duration)

              //返回结果
              override def getResult(accumulator: (Long, Long)): Double = (accumulator._2 / accumulator._1).toDouble

              //合并累加器
              override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = (a._1 + b._1, a._2 + b._2)
            }, classOf[Tuple2[Long, Long]])

          //获取状态
          aggregatingState = getRuntimeContext.getAggregatingState(aggregatingStateDescriptor)
        }

        override def map(stationLog: StationLog): String = {
          aggregatingState.add(stationLog)
          stationLog.callOut + "的平均通话时长是：" + aggregatingState.get()
        }
      }).print()

    env.execute()
  }

}
