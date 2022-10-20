package org.apache.flink.lakesoul;


import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.entity.TableNameId;
import com.dmetasoul.lakesoul.meta.external.DBConnector;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;

import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class OracleDBManager {

    private static final String EXTERNAL_ORACLE_TABLE_PREFIX = "external_oracle_table_";


    public static final int DEFAULT_ORACLE_PORT = 1521;
    private final DBConnector dbConnector;
    private final SparkSession spark;


    private final DBManager lakesoulDBManager = new DBManager();
    private final String lakesoulTablePathPrefix;
    private final String dbName;
    private final int hashBucketNum;
    private final boolean useCdc;

    private final DataBaseProperty dataBaseProperty;
    private HashSet<String> excludeTables;
    private HashSet<String> includeTables;

    OracleDataTypeConverter converter = new OracleDataTypeConverter();

    OracleDdlParser parser = new OracleDdlParser();

    private String[] filterTables = new String[]{};

    public OracleDBManager(String dbName,
                           String user,
                           String passwd,
                           String host,
                           String port, HashSet<String> excludeTables,
                           HashSet<String> includeTables,
                           String pathPrefix, int hashBucketNum, boolean useCdc
    ) {
        this.dbName = dbName;
        this.excludeTables = excludeTables;
        this.includeTables = includeTables;
        excludeTables.addAll(Arrays.asList(filterTables));


        dataBaseProperty = new DataBaseProperty();
        dataBaseProperty.setDriver("oracle.jdbc.driver.OracleDriver");
        String url = "jdbc:oracle:thin:@" + host + ":" + port + "/" + dbName;
        dataBaseProperty.setUrl(url);
        dataBaseProperty.setUsername(user);
        dataBaseProperty.setPassword(passwd);
        dbConnector = new DBConnector(dataBaseProperty);

        spark = SparkSession
                .builder()
                .master("local[1]")
                .getOrCreate();

        lakesoulTablePathPrefix = pathPrefix;
        this.hashBucketNum = hashBucketNum;
        this.useCdc = useCdc;
    }

    public boolean checkOpenStatus() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select status from v$instance";
        boolean opened = false;
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                opened = rs.getString("STATUS").equals("OPEN");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return opened;
    }

    public List<String> listTables() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "SELECT table_name FROM user_tables";
        List<String> list = new ArrayList<>();
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                list.add(tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public List<String> listNamespace() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select NAME from v$database";
        List<String> list = new ArrayList<>();
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String database = rs.getString("NAME");
                list.add(database);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public String showCreateTable(String tableName) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select table_name,dbms_metadata.get_ddl('TABLE','%s') as DDL from dual,user_tables where table_name='%s'", tableName.toUpperCase(), tableName.toUpperCase());
        String result = null;
        try {
            conn = dbConnector.getConn();
            System.out.println(sql);
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                result = rs.getString("DDL");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return result;
    }

    public void importOrSyncLakeSoulTable(String tableName) {
        if (!includeTables.contains(tableName) && excludeTables.contains(tableName)) {
            System.out.println(String.format("Table %s is excluded by exclude table list", tableName));
            return;
        }
//        String ddl = showCreateTable(tableName);
//        System.out.println(ddl);
        StructType schema = spark
                .read()
                .format("jdbc")
                .option("url", dataBaseProperty.getUrl())
                .option("dbtable", tableName)
                .option("user", dataBaseProperty.getUsername())
                .option("password", dataBaseProperty.getPassword())
                .option("driver", dataBaseProperty.getDriver())
                .load()
                .schema();


        boolean exists = lakesoulDBManager.isTableExistsByTableName(tableName, dbName);
        if (exists) {
            // sync lakesoul table schema only
            TableNameId tableId = lakesoulDBManager.shortTableName(tableName, dbName);
            String newTableSchema = schema.json();

            lakesoulDBManager.updateTableSchema(tableId.getTableId(), newTableSchema);
        } else {
            // import new lakesoul table with schema, pks and properties
            try {
                String tableId = EXTERNAL_ORACLE_TABLE_PREFIX + UUID.randomUUID();

                String qualifiedPath =
                        FlinkUtil.makeQualifiedPath(new Path(new Path(
                                lakesoulTablePathPrefix, dbName
                        ), tableName)).toString();


                String tableSchema = schema.json();
                List<String> priKeys = List.of("");
                String partitionsInTableInfo = ";" + String.join(",", priKeys);
                JSONObject json = new JSONObject();
                json.put("hashBucketNum", String.valueOf(hashBucketNum));
                json.put("lakesoul_cdc_change_column", "rowKinds");

                lakesoulDBManager.createNewTable(tableId, dbName, tableName, qualifiedPath,
                        tableSchema,
                        json, partitionsInTableInfo
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void importOrSyncLakeSoulNamespace(String namespace) {

    }

    public Tuple2<StructType, List<String>> ddlToSparkSchema(String tableName, String ddl) {
        final StructType[] stNew = {new StructType()};

        parser.parse(ddl, new Tables());
        Table table = parser.databaseTables().forTable(null, null, tableName);
        table.columns()
                .forEach(col -> {
                    String name = col.name();
                    DataType datatype = converter.schemaBuilder(col);
                    if (datatype == null) {
                        throw new IllegalStateException("Unhandled data types");
                    }
                    stNew[0] = stNew[0].add(name, datatype, col.isOptional());
                });
        //if uescdc add lakesoulcdccolumns
        if (useCdc) {
            stNew[0] = stNew[0].add("rowKinds", DataTypes.StringType, true);
        }
        return Tuple2.of(stNew[0], table.primaryKeyColumnNames());
    }
}