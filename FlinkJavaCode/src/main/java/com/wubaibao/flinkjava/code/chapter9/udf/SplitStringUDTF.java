package com.wubaibao.flinkjava.code.chapter9.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitStringUDTF extends TableFunction<Row> {
    public void eval(String str) {
        String[] split = str.split("\\|");
        for (String s : split) {
            collect(Row.of(s,s.length()));
        }
    }
}
