package com.wubaibao.flinkjava.code.chapter9.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * 自定义表聚合函数，获取每个基站通话时长top2
 * TableAggregateFunction<T,ACC>: T-最终聚合结果类型，ACC-累加器类型
 */
public class Top2DurationTableUDAF extends TableAggregateFunction<Tuple2<Long,Integer>,Tuple2<Long,Long>> {

    //创建累加器，累加器存储最大值和次大值
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return Tuple2.of(Long.MIN_VALUE,Long.MIN_VALUE);
    }

    //累加器的计算逻辑：判断传入的duration是否大于累加器中的最大值或者次大值，如果大于则替换
    public void accumulate(Tuple2<Long, Long> acc, Long duration){
        if(duration>acc.f0){
            acc.f1 = acc.f0;
            acc.f0 = duration;
        }else if(duration>acc.f1){
            acc.f1 = duration;
        }

    }

    //返回结果,将累加器中的最大值和次大值返回
    public void emitValue(Tuple2<Long, Long> acc, Collector<Tuple2<Long, Integer>> out){
        if(acc.f0!=Long.MIN_VALUE){
            out.collect(Tuple2.of(acc.f0,1));
        }
        if(acc.f1!=Long.MIN_VALUE){
            out.collect(Tuple2.of(acc.f1,2));
        }
    }

}
