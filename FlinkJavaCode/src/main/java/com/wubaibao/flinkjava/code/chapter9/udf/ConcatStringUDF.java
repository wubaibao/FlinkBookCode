package com.wubaibao.flinkjava.code.chapter9.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 自定义标量函数，将多个字段拼接为一个字符串输出
 */
public class ConcatStringUDF extends ScalarFunction {
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... args) {
        StringBuilder sb = new StringBuilder();
        for (Object arg : args) {
            sb.append(arg.toString()+"|");
        }
        return sb.substring(0,sb.toString().length()-1);
    }
}

