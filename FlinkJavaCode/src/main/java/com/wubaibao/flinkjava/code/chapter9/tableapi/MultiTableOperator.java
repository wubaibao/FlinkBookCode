package com.wubaibao.flinkjava.code.chapter9.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * Flink Table API 多表连接操作
 *
 */
public class MultiTableOperator {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        Table t1 = tableEnv.fromValues(
                row(1, "zs", 18),
                row(1, "zs", 18),
                row(2, "ls", 19),
                row(2, "ls", 19),
                row(3, "ww", 20),
                row(4, "ml", 21),
                row(5, "tq", 22)
        );

        Table t2 = tableEnv.fromValues(
                row(1, "zs", 18),
                row(2, "ls", 19),
                row(2, "ls", 19),
                row(3, "ww", 20),
                row(3, "ww", 20),
                row(4, "ml", 21),
                row(6, "gb", 23)
        );

        //打印Schema
        t1.printSchema();
        t2.printSchema();

        // 1. union
        Table union = t1.union(t2);
        union.execute().print();

        // 2. unionAll
        Table unionAll = t1.unionAll(t2);
        unionAll.execute().print();

        // 3. intersect
        Table intersect = t1.intersect(t2);
        intersect.execute().print();

        // 4. intersectAll
        Table intersectAll = t1.intersectAll(t2);
        intersectAll.execute().print();

        // 5. minus
        Table minus = t1.minus(t2);
        minus.execute().print();

        // 6. minusAll
        Table minusAll = t1.minusAll(t2);
        minusAll.execute().print();

        // 7. In
        Table in = t1.select($("f0"),$("f1"),$("f2")).where($("f0").in(2,3));
        in.execute().print();

    }
}
