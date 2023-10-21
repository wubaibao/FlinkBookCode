package com.wubaibao.flinkjava.code.chapter6.kryoser;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 用户自定义Kryo序列化测试
 */
public class KryoSerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //注册自定义的Kryo序列化类
        env.getConfig().registerTypeWithKryoSerializer(Student.class, StudentSerializer.class);

        //用户基本信息
        env.fromCollection(Arrays.asList(
                        "1,zs,18",
                        "2,ls,20",
                        "3,ww,19"
                )).map(one -> {
                    String[] split = one.split(",");
                    return new Student(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
                }).returns(Types.GENERIC(Student.class))
                .filter(new FilterFunction<Student>() {
                    @Override
                    public boolean filter(Student student) throws Exception {
                        return student.id > 1;
                    }
                })
                .print();

        env.execute();

    }

}

