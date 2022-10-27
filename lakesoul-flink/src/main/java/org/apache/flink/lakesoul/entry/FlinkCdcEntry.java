package org.apache.flink.lakesoul.entry;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.dmetasoul.lakesoul.meta.external.oracle.OracleDBManager;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.sink.LakeSoulOracleMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.source.OracleSourceBuild;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;
import java.util.List;
import java.util.Arrays;


import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BUCKET_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SOURCE_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.WAREHOUSE_PATH;

public class FlinkCdcEntry {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String cdcSourceType = parameter.get(SOURCE_DB_TYPE_NAME.key(), SOURCE_DB_TYPE_NAME.defaultValue());
        String dbName = parameter.get(SOURCE_DB_DB_NAME.key());
        String userName = parameter.get(SOURCE_DB_USER.key());
        String passWord = parameter.get(SOURCE_DB_PASSWORD.key());
        String host = parameter.get(SOURCE_DB_HOST.key());
        int port = parameter.getInt(SOURCE_DB_PORT.key(), MysqlDBManager.DEFAULT_MYSQL_PORT);
        String databasePrefixPath = parameter.get(WAREHOUSE_PATH.key());
        String serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key());
        int bucketParallelism = parameter.getInt(BUCKET_PARALLELISM.key());
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());     //mill second

        // import source database catalog info
        Configuration conf = new Configuration();
        new DBManager().cleanMeta();

        ExternalDBManager dbManager = null;
        if (cdcSourceType.equals("mysql")) {

            dbManager = new MysqlDBManager(dbName,
                    userName,
                    passWord,
                    host,
                    Integer.toString(port),
                    new HashSet<>(),
                    databasePrefixPath,
                    bucketParallelism,
                    true);

            dbManager.importOrSyncLakeSoulNamespace(dbName);
        } else if (cdcSourceType.equals("oracle_11g")) {
            dbManager = new OracleDBManager(dbName,
                    userName,
                    passWord,
                    host,
                    Integer.toString(port),
                    new HashSet<>(Arrays.asList("SYS_IOT_OVER_74800", "LOG_MINING_FLUSH")),
                    new HashSet<>(),
                    databasePrefixPath,
                    bucketParallelism,
                    true);
        }

        assert dbManager != null;

        List<String> tableList = dbManager.listTables();
        if (tableList.isEmpty()) {
            throw new IllegalStateException("Failed to discover captured tables");
        }
        tableList.forEach(dbManager::importOrSyncLakeSoulTable);

        // parameters for mutil tables ddl sink
        conf.set(SOURCE_DB_TYPE_NAME, cdcSourceType);
        conf.set(SOURCE_DB_DB_NAME, dbName);
        conf.set(SOURCE_DB_USER, userName);
        conf.set(SOURCE_DB_PASSWORD, passWord);
        conf.set(SOURCE_DB_HOST, host);
        conf.set(SOURCE_DB_PORT, port);
        conf.set(WAREHOUSE_PATH, databasePrefixPath);
        conf.set(SERVER_TIME_ZONE, serverTimezone);

        // parameters for mutil tables dml sink
        conf.set(LakeSoulSinkOptions.USE_CDC, true);
        conf.set(LakeSoulSinkOptions.WAREHOUSE_PATH, databasePrefixPath);
        conf.set(LakeSoulSinkOptions.SOURCE_PARALLELISM, sourceParallelism);
        conf.set(LakeSoulSinkOptions.BUCKET_PARALLELISM, bucketParallelism);

        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);

        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);

        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if (parameter.get(JOB_CHECKPOINT_MODE.key(), JOB_CHECKPOINT_MODE.defaultValue()).equals("AT_LEAST_ONCE")) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);

        env.getCheckpointConfig().setCheckpointStorage(parameter.get(FLINK_CHECKPOINT.key()));
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);


        if (cdcSourceType.equals("mysql")) {
            MySqlSourceBuilder<JsonSourceRecord> sourceBuilder = MySqlSource.<JsonSourceRecord>builder()
                    .hostname(host)
                    .port(port)
                    .databaseList(dbName) // set captured database
                    .tableList(dbName + ".*") // set captured table
                    .serverTimeZone(serverTimezone)  // default -- Asia/Shanghai
                    .username(userName)
                    .password(passWord);

            LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
            context.env = env;
            context.sourceBuilder = sourceBuilder;
            context.conf = conf;
            LakeSoulMultiTableSinkStreamBuilder builder = new LakeSoulMultiTableSinkStreamBuilder(context);
            DataStreamSource<JsonSourceRecord> source = builder.buildMultiTableSource();
            Tuple2<DataStream<JsonSourceRecord>, DataStream<JsonSourceRecord>> streams =
                    builder.buildCDCAndDDLStreamsFromSource(source);
            DataStream<JsonSourceRecord> stream = builder.buildHashPartitionedCDCStream(streams.f0);
            DataStreamSink<JsonSourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
            DataStreamSink<JsonSourceRecord> ddlSink = builder.buildLakeSoulDDLSink(streams.f1);

        } else if (cdcSourceType.equals("oracle_11g")) {
            String schemaName = userName.toUpperCase();
            dbManager.importOrSyncLakeSoulNamespace(dbName + "." + schemaName);
            OracleSourceBuild<JsonSourceRecord> sourceBuilder = OracleSourceBuild.<JsonSourceRecord>builder()
                    .hostname(host)
                    .port(port)
                    .database(dbName) // set captured database
                    .tableList(schemaName+".*") // set captured table
                    .schemaList(schemaName)
//                        .serverTimeZone(serverTimezone)  // default -- Asia/Shanghai
                    .username(userName)
                    .password(passWord);
            LakeSoulOracleMultiTableSinkStreamBuilder.Context context = new LakeSoulOracleMultiTableSinkStreamBuilder.Context();
            context.env = env;
            context.sourceBuilder = sourceBuilder;
            context.conf = conf;
            LakeSoulOracleMultiTableSinkStreamBuilder builder = new LakeSoulOracleMultiTableSinkStreamBuilder(context);
            DataStreamSource<JsonSourceRecord> source = builder.buildMultiTableSource();
            Tuple2<DataStream<JsonSourceRecord>, DataStream<JsonSourceRecord>> streams =
                    builder.buildCDCAndDDLStreamsFromSource(source);
            DataStream<JsonSourceRecord> stream = builder.buildHashPartitionedCDCStream(streams.f0);
            DataStreamSink<JsonSourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
            DataStreamSink<JsonSourceRecord> ddlSink = builder.buildLakeSoulDDLSink(streams.f1);
        }

        env.execute("LakeSoul CDC Sink From MySQL Database " + dbName);
    }
}
