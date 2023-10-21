package com.wubaibao.flinkscala.code.chapter7.operatorstate

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.io.{BufferedWriter, FileWriter}
import java.util
import java.util.UUID
import scala.collection.mutable.ListBuffer

/**
 * Flink Operator State 状态编程 - ListState测试
 * 需要实现CheckpointedFunction接口，重写snapshotState和initializeState方法
 * 案例：将所有通话信息写出到本地文件系统中，通过CheckpointedFunction接口保证数据的一致性。
 */
object OperatorStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 开启Checkpoint，每隔5秒钟做一次Checkpoint
    env.enableCheckpointing(5000)

    // 为了能看到效果，这里设置并行度为2
    env.setParallelism(2)

    /**
     * Socket中数据如下:
     *  001,186,187,busy,1000,10
     *  002,187,186,fail,2000,20
     *  003,186,188,busy,3000,30
     *  004,187,186,busy,4000,40
     *  005,189,187,busy,5000,50
     */
    val ds = env.socketTextStream("node5", 9999)

    // 对ds进行转换处理，得到StationLog对象
    val stationLogDS: DataStream[StationLog] = ds.map(new MapFunction[String, StationLog] {
      override def map(line: String): StationLog = {
        val arr = line.split(",")
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

    stationLogDS.map(new MyMapAndCheckpointedFunction).print()

    env.execute()
  }
}

class MyMapAndCheckpointedFunction extends RichMapFunction[StationLog, String] with CheckpointedFunction {
  // 定义ListState 用于存储每个并行度中的StationLog对象
  private var stationLogListState: ListState[StationLog] = _

  // 定义一个本地集合，用于存储当前并行度中的所有StationLog对象
  private val stationLogList: ListBuffer[StationLog] = new ListBuffer[StationLog]()

  override def map(stationLog: StationLog): String = {
    // 每当有一条数据进来，就将数据保存到本地集合中
    stationLogList.append(stationLog)

    // 当本地集合中的数据条数达到3条时，将数据写出到本地文件系统中
    if (stationLogList.size == 3) {
      // 获取当前线程的ID
      val indexOfThisSubtask = getRuntimeContext.getIndexOfThisSubtask

      // 生成文件名
      val uuid = UUID.randomUUID().toString
      val fileName = s"$indexOfThisSubtask-$uuid.txt"

      // 将数据写出到本地文件系统中
      writeDataToLocalFile(stationLogList, fileName)
      // 清空本地集合中的数据
      stationLogList.clear()

      // 返回写出的结果
      s"成功将数据写出到本地文件系统中，文件名为：$fileName"
    } else {
      s"当前subtask中的数据条数为：${stationLogList.size}, 不满足写出条件"
    }
  }

  // 将数据写出到本地文件系统中，每次写出文件名通过UUID生成
  private def writeDataToLocalFile(stationLogList: ListBuffer[StationLog], fileName: String): Unit = {
    val writer = new BufferedWriter(new FileWriter(fileName))
    // 将数据写出到本地文件系统中
    for (stationLog <- stationLogList) {
      // 写出数据
      writer.write(stationLog.toString + "\n")
      // flush
      writer.flush()
    }
    writer.close()
  }

  // 该方法会在checkpoint触发时调用，checkpoint触发时，将List中的数据保存到状态中
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println("checkpoint 方法调用了，将要把list中的数据保存到状态中")
    // 清空状态中的数据
    stationLogListState.clear()

    // 将本地集合中的数据保存到状态中
    import scala.collection.JavaConverters._
    stationLogListState.addAll(stationLogList.asJava)
  }

  // 该方法会在自定义函数初始化时调用，调用此方法初始化状态和恢复状态
  override def initializeState(context: FunctionInitializationContext): Unit = {
    println("initializeState 方法调用了，初始化状态和恢复状态")

    // 创建ListStateDescriptor 状态描述器
    val listStateDescriptor = new ListStateDescriptor[StationLog]("listState", classOf[StationLog])
    // 通过状态描述器获取状态
    stationLogListState = context.getOperatorStateStore.getListState(listStateDescriptor)

    // isRestored()方法用于判断程序是否是恢复状态，如果是恢复状态，返回true，否则返回false
    if (context.isRestored) {
      // 如果是恢复状态，从状态中恢复数据
      import scala.collection.JavaConverters._
      for (stationLog <- stationLogListState.get.asScala) {
        stationLogList.append(stationLog)
      }
    }
  }

}