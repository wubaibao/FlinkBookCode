package com.wubaibao.flinkjava.code.chapter9.udf;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * 自定义聚合函数，计算平均通话时长
 * 泛型：<T,ACC>: T-最终返回的结果类型，ACC-累加器类型
 */
public class AvgDurationUDAF extends AggregateFunction<Double,Tuple2<Long,Integer>> {

    //初始化累加器
    @Override
    public Tuple2<Long, Integer> createAccumulator() {
        return Tuple2.of(0L,0);
    }

    //累加器的计算逻辑
    public void accumulate(Tuple2<Long, Integer> acc, Long duration){
        //累加器第一个字段为总通话时长，第二个字段为通话次数
        acc.f0 += duration;
        acc.f1 += 1;
    }

    //返回结果
    @Override
    public Double getValue(Tuple2<Long, Integer> accumulator) {
        if(accumulator.f1 == 0){
            return null;
        }else {
            return accumulator.f0*1.0/accumulator.f1;
        }
    }

}
