package com.wubaibao.flinkscala.code.chapter7.keyedstate

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink KeyedState TTL测试
 * 案例：读取Socket中基站通话数据，统计每个主叫通话总时长
 */
object TTLTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * Socket中数据如下:
     *  001,186,187,busy,1000,10
     *  002,187,186,fail,2000,20
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    // 对ds进行转换处理，得到StationLog对象
    val stationLogDS: DataStream[StationLog] = ds.map(line => {
      val arr = line.split(",")
      StationLog(
        arr(0).trim,
        arr(1).trim,
        arr(2).trim,
        arr(3).trim,
        arr(4).toLong,
        arr(5).toLong
      )
    })

    stationLogDS.keyBy(_.callOut)
      .map(new RichMapFunction[StationLog, String]() {
        // 定义ValueState 用来存放同一个主叫号码的通话总时长
        private var valueState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          // 定义状态TTL
          val ttlConfig = StateTtlConfig
            // 设置状态有效期为10秒
            .newBuilder(org.apache.flink.api.common.time.Time.seconds(10))
            // 设置状态更新类型为OnCreateAndWrite，即状态TTL创建和写入时更新
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            // 设置状态可见性为NeverReturnExpired，即状态过期后不返回
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()

          // 定义状态描述器
          val descriptor = new ValueStateDescriptor[Long]("value-state", classOf[Long])

          // 设置状态TTL
          descriptor.enableTimeToLive(ttlConfig)

          // 获取状态
          valueState = getRuntimeContext.getState(descriptor)
        }

        override def map(stationLog: StationLog): String = {
          val stateValue = valueState.value
          if (stateValue == null) {
            // 如果状态值为null，说明是第一次使用，直接更新状态值
            valueState.update(stationLog.duration)
          } else {
            // 如果状态值不为null，说明不是第一次使用，需要累加通话时长
            valueState.update(stateValue + stationLog.duration)
          }

          stationLog.callOut + "通话总时长：" + valueState.value + "秒"
        }
      }).print()

    env.execute()
  }

}
