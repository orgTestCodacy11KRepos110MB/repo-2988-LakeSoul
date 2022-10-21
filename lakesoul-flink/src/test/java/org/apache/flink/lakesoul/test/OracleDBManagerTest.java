package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.external.oracle.OracleDBManager;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

import java.util.HashSet;

public class OracleDBManagerTest {

    protected  DBManager lakesoulDBManager = new DBManager();
    private  String dbName;
    private  String userName;
    private  String passWord;
    private  String host;
    private  int port;
    private  String databasePrefixPath;
    private  int bucketParallelism;

    private static OracleDBManager dbManager;

    private final String[] args="--source_db.db_name helowinXDB --source_db.user test --source_db.password test --source_db.host localhost --source_db.port 1521 --warehouse_path file:///Users/ceng/lakesoul --hash_bucket_num 2 --job.checkpoint_interval 5000 --source.parallelism 1 --sink.parallelism 1 --flink.checkpoint file:///Users/ceng/lakesoul --flink.savepoint file:///Users/ceng/lakesoul/savepoint".split(" ");



    @Before
    public void init() {
        lakesoulDBManager.cleanMeta();
        ParameterTool parameter = ParameterTool.fromArgs(args);
        dbName = parameter.get(LakeSoulDDLSinkOptions.SOURCE_DB_DB_NAME.key());
        userName = parameter.get(LakeSoulDDLSinkOptions.SOURCE_DB_USER.key());
        passWord = parameter.get(LakeSoulDDLSinkOptions.SOURCE_DB_PASSWORD.key());
        host = parameter.get(LakeSoulDDLSinkOptions.SOURCE_DB_HOST.key());
        port = parameter.getInt(LakeSoulDDLSinkOptions.SOURCE_DB_PORT.key(), OracleDBManager.DEFAULT_ORACLE_PORT);
        databasePrefixPath = parameter.get(LakeSoulSinkOptions.WAREHOUSE_PATH.key());
        bucketParallelism = parameter.getInt(LakeSoulSinkOptions.BUCKET_PARALLELISM.key(), LakeSoulSinkOptions.BUCKET_PARALLELISM.defaultValue());
        dbManager = new OracleDBManager(dbName,
                userName,
                passWord,
                host,
                Integer.toString(port),
                new HashSet<>(),
                new HashSet<>(),
                databasePrefixPath,
                bucketParallelism,
                true);
    }

    @Test
    public void importOrSyncLakeSoulTable() {
        Assert.assertTrue(dbManager.checkOpenStatus());
        Assert.assertFalse(dbManager.listNamespace().isEmpty());
        Assert.assertFalse(dbManager.listTables().isEmpty());
        Assert.assertEquals(lakesoulDBManager.getTableInfoByNameAndNamespace("COUNTRIES_DEMO",dbName), null);
        dbManager.importOrSyncLakeSoulTable("COUNTRIES_DEMO");
        Assert.assertEquals(lakesoulDBManager.getTableInfoByNameAndNamespace("COUNTRIES_DEMO",dbName).getTableName(), "COUNTRIES_DEMO");
    }
}