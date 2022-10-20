package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.lakesoul.OracleDBManager;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;

import java.util.HashSet;

public class OracleDBManagerTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String dbName = parameter.get(LakeSoulDDLSinkOptions.SOURCE_DB_DB_NAME.key());
        String userName = parameter.get(LakeSoulDDLSinkOptions.SOURCE_DB_USER.key());
        String passWord = parameter.get(LakeSoulDDLSinkOptions.SOURCE_DB_PASSWORD.key());
        String host = parameter.get(LakeSoulDDLSinkOptions.SOURCE_DB_HOST.key());
        int port = parameter.getInt(LakeSoulDDLSinkOptions.SOURCE_DB_PORT.key(), OracleDBManager.DEFAULT_ORACLE_PORT);
        String databasePrefixPath = parameter.get(LakeSoulSinkOptions.WAREHOUSE_PATH.key());
        int sourceParallelism = parameter.getInt(LakeSoulSinkOptions.SOURCE_PARALLELISM.key(), LakeSoulSinkOptions.SOURCE_PARALLELISM.defaultValue());
        int bucketParallelism = parameter.getInt(LakeSoulSinkOptions.BUCKET_PARALLELISM.key(), LakeSoulSinkOptions.BUCKET_PARALLELISM.defaultValue());
        int checkpointInterval = parameter.getInt(JobOptions.JOB_CHECKPOINT_INTERVAL.key(),
                JobOptions.JOB_CHECKPOINT_INTERVAL.defaultValue());     //mill second

        DBManager lakesoulDBManager = new DBManager();
        lakesoulDBManager.cleanMeta();

        OracleDBManager dbManager = new OracleDBManager(dbName,
                                                        userName,
                                                        passWord,
                                                        host,
                                                        Integer.toString(port),
                                                        new HashSet<>(),
                                                        new HashSet<>(),
                                                        databasePrefixPath,
                                                        bucketParallelism,
                                                        true);
        System.out.println(dbManager.checkOpenStatus());
        dbManager.listNamespace().forEach(System.out::println);
        dbManager.listTables().forEach(System.out::println);
        dbManager.importOrSyncLakeSoulTable("COUNTRIES_DEMO");
    }
}