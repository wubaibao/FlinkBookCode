package com.wubaibao.flinkscala.code.chapter6

/**
 * StationLog基站日志类
 * sid:基站ID
 * callOut: 主叫号码
 * callIn: 被叫号码
 * callType: 通话类型，失败（fail）/占线（busy）/拒接（barring）/接通（success）
 * callTime: 呼叫时间戳，毫秒
 * duration: 通话时长，秒
 */
case class StationLog(sid:String,callOut:String,callIn:String,var callType:String,
                      callTime:Long,duration:Long)

