package com.wubaibao.flinkjava.code.chapter7.operatorstate;

import com.wubaibao.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Flink Operator State 状态编程 -  ListState测试
 * 需要实现CheckpointedFunction接口，重写snapshotState和initializeState方法
 * 案例：将读取到的基站通话日志写出到本地文件中，通过CheckpointedFunction接口保证数据的一致性。
 */
public class OperatorStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启Checkpoint，每隔5秒钟做一次Checkpoint
        env.enableCheckpointing(5000);

        //为了能看到效果，这里设置并行度为2
        env.setParallelism(2);

        /**
         * Socket中数据如下:
         *  001,186,187,busy,1000,10
         *  002,187,186,fail,2000,20
         *  003,186,188,busy,3000,30
         *  004,187,186,busy,4000,40
         *  005,189,187,busy,5000,50
         */
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //对ds进行转换处理，得到StationLog对象
        SingleOutputStreamOperator<StationLog> stationLogDS = ds.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String line) throws Exception {
                String[] arr = line.split(",");
                return new StationLog(
                        arr[0].trim(),
                        arr[1].trim(),
                        arr[2].trim(),
                        arr[3].trim(),
                        Long.valueOf(arr[4]),
                        Long.valueOf(arr[5])
                );
            }
        });

        stationLogDS.map(new MyMapAndCheckpointedFunction()).print();

        env.execute();
    }
}
class MyMapAndCheckpointedFunction extends RichMapFunction<StationLog, String> implements CheckpointedFunction{
    //定义ListState 用于存储每个并行度中的StationLog对象
    private ListState<StationLog> stationLogListState;

    //定义一个本地集合，用于存储当前并行度中的所有StationLog对象
    private List<StationLog> stationLogList = new ArrayList<>();

    @Override
    public String map(StationLog stationLog) throws Exception {
        //每当有一条数据进来，就将数据保存到本地集合中
        stationLogList.add(stationLog);

        //当本地集合中的数据条数达到3条时，将数据写出到本地文件系统中
        if (stationLogList.size() == 3){
            //获取当前线程的ID
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

            //生成文件名
            String uuid = UUID.randomUUID().toString();
            String fileName = indexOfThisSubtask+"-"+uuid+".txt";

            //将数据写出到本地文件系统中
            writeDataToLocalFile(stationLogList,fileName);
            //清空本地集合中的数据
            stationLogList.clear();

            //返回写出的结果
            return "成功将数据写出到本地文件系统中,文件名为："+fileName;
        }else{
            return "当前subtask中的数据条数为："+stationLogList.size()+",不满足写出条件";
        }
    }

    //将数据写出到本地文件系统中，每次写出文件名通过UUID生成
    private void writeDataToLocalFile(List<StationLog> stationLogList,String fileName) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        //将数据写出到本地文件系统中
        for (StationLog stationLog : stationLogList) {
            //写出数据
            writer.write(stationLog.toString()+"\n");
            //flush
            writer.flush();
        }
    }


    // 该方法会在checkpoint触发时调用，checkpoint触发时，将List中的数据保存到状态中
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("checkpoint 方法调用了，将要把list中的数据保存到状态中");
        //清空状态中的数据
        stationLogListState.clear();

        //将本地集合中的数据保存到状态中
        stationLogListState.addAll(stationLogList);
    }

    // 该方法会在自定义函数初始化时调用，调用此方法初始化状态和恢复状态
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState 方法调用了，初始化状态和恢复状态");

        //创建ListStateDescriptor 状态描述器
        ListStateDescriptor<StationLog> listStateDescriptor = new ListStateDescriptor<>("listState", StationLog.class);
        //通过状态描述器获取状态
        stationLogListState = context.getOperatorStateStore().getListState(listStateDescriptor);

        //isRestored()方法用于判断程序是否是恢复状态，如果是恢复状态，返回true，否则返回false
        if (context.isRestored()){
            //如果是恢复状态，从状态中恢复数据
            for (StationLog stationLog : stationLogListState.get()) {
                stationLogList.add(stationLog);
            }

        }
    }
}