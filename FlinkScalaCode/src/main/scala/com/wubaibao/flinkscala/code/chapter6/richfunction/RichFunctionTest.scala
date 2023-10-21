package com.wubaibao.flinkscala.code.chapter6.richfunction

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * Flink RichFunction测试
 */
object RichFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._

    /**
     * Socket中的数据格式如下:
     *  001,186,187,busy,1000,10
     *  002,187,186,fail,2000,20
     *  003,186,188,busy,3000,30
     *  004,188,186,busy,4000,40
     *  005,188,187,busy,5000,50
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    ds.map(new MyRichMapFunction).print()

    env.execute()
  }

  private class MyRichMapFunction extends RichMapFunction[String, String] {
    private var conn: Connection = _
    private var pst: PreparedStatement = _
    private var rst: ResultSet = _

    // open()方法在map方法之前执行，用于初始化
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://node2:3306/mydb?useSSL=false", "root", "123456")
      pst = conn.prepareStatement("select * from person_info where phone_num = ?")
    }

    // map方法，输入一个元素，返回一个元素
    override def map(value: String): String = {
      //value 格式：001,186,187,busy,1000,10
      val split: Array[String] = value.split(",")
      val sid: String = split(0)
      val callOut: String = split(1) //主叫
      val callIn: String = split(2) //被叫
      val callType: String = split(3) //通话类型
      val callTime: String = split(4) //通话时间
      val duration: String = split(5) //通话时长
      //mysql中获取主叫和被叫的姓名
      var callOutName = ""
      var callInName = ""

      pst.setString(1, callOut)
      rst = pst.executeQuery()
      while (rst.next()) {
        callOutName = rst.getString("name")
      }

      pst.setString(1, callIn)
      rst = pst.executeQuery()
      while (rst.next()) {
        callInName = rst.getString("name")
      }

      s"基站ID:$sid,主叫号码:$callOut,主叫姓名:$callOutName," +
        s"被叫号码:$callIn,被叫姓名:$callInName,通话类型:$callType," +
        s"通话时间:$callTime,通话时长:$duration"
    }

    // close()方法在map方法之后执行，用于清理
    override def close(): Unit = {
      if (rst != null) rst.close()
      if (pst != null) pst.close()
      if (conn != null) conn.close()
    }
  }
}
