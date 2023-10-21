package com.wubaibao.flinkjava.code.chapter9.time;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;

/**
 * Flink 时间和时区演示
 */
public class TimeZoneTest {
    public static void main(String[] args) {
        //准备环境配置
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        //创建TableEnvironment
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //设置时区
        //tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        Table table1 = tableEnv.sqlQuery("" +
                "SELECT " +
                "TIMESTAMP '1970-01-01 00:00:01.001' AS ntz," +
                "TO_TIMESTAMP_LTZ(4001, 3) AS ltz"
        );
        table1.printSchema();
        table1.execute().print();

        //转换数据
        Table table2 = tableEnv.sqlQuery("" +
                "SELECT " +
                "CAST (ltz AS TIMESTAMP_LTZ(3)) AS cast_ltz_3," +
                "CAST (ltz AS TIMESTAMP_LTZ(6)) AS cast_ltz_6," +
                "CAST (ntz AS TIMESTAMP(3)) AS cast_ntz_3," +
                "CAST (ntz AS TIMESTAMP(6)) AS cast_ntz_6 " +
                "from "+table1
        );
        table2.printSchema();
        table2.execute().print();

    }
}
