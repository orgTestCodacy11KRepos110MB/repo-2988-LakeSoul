package com.dmetasoul.lakesoul.meta.external.oracle;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.entity.TableNameId;
import com.dmetasoul.lakesoul.meta.external.DBConnector;
import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;

import io.debezium.util.Collect;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.*;

public class OracleDBManager implements ExternalDBManager {

    private static final String EXTERNAL_ORACLE_TABLE_PREFIX = "external_oracle_table_";


    public static final int DEFAULT_ORACLE_PORT = 1521;
    private final DBConnector dbConnector;
//    private final SparkSession spark;


    private final DBManager lakesoulDBManager = new DBManager();
    private final String lakesoulTablePathPrefix;
    private final String dbName;
    private final int hashBucketNum;
    private final boolean useCdc;

    private final DataBaseProperty dataBaseProperty;
    private HashSet<String> excludeTables;
    private HashSet<String> includeTables;

    OracleDataTypeConverter converter = new OracleDataTypeConverter();

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

//        spark = SparkSession
//                .builder()
//                .master("local[1]")
//                .getOrCreate();

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

    @Override
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

    @Override
    public void importOrSyncLakeSoulTable(String tableName) {
        if (!includeTables.contains(tableName) && excludeTables.contains(tableName)) {
            System.out.println(String.format("Table %s is excluded by exclude table list", tableName));
            return;
        }

        List<Column> columns = readSchema(tableName);
        List<String> priKeys = readPrimaryKeyNames(tableName);
        StructType schema = columnsToSparkSchema(columns);
        System.out.println(priKeys);

        if (priKeys.isEmpty()) {
            throw new IllegalStateException(String.format("Table %s has no primary key, table with no Primary Keys is not supported", tableName));
        }

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
                String partitionsInTableInfo = ";" + String.join(",", priKeys);
                JSONObject json = new JSONObject();
                json.put("hashBucketNum", String.valueOf(hashBucketNum));
                json.put("lakesoul_cdc_change_column", "rowKinds");

                lakesoulDBManager.createNewTable(tableId, dbName, tableName, qualifiedPath,
                        tableSchema,
                        json, partitionsInTableInfo
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void importOrSyncLakeSoulNamespace(String namespace) {
        if (lakesoulDBManager.getNamespaceByNamespace(namespace) != null) {
            return;
        }
        lakesoulDBManager.createNewNamespace(namespace, new JSONObject(), "");
    }


    public StructType columnsToSparkSchema(List<Column> columns) {
        final StructType[] stNew = {new StructType()};

        columns.forEach(col -> {
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
        return stNew[0];
    }

    private List<Column> readSchema(String tableName){
        Connection conn = null;
        List<Column> list = new ArrayList<>();
        ResultSet columnMetadata = null;
        try {

            conn = dbConnector.getConn();
            DatabaseMetaData metadata = conn.getMetaData();
            columnMetadata = metadata.getColumns("", dataBaseProperty.getUsername().toUpperCase(), tableName, "%");
            while (columnMetadata.next()) {
                String defaultValue = columnMetadata.getString(13);
                String columnName = columnMetadata.getString(4);

                ColumnEditor column = Column.editor().name(columnName);
                column.type(columnMetadata.getString(6));
                column.length(columnMetadata.getInt(7));
                if (columnMetadata.getObject(9) != null) {
                    column.scale(columnMetadata.getInt(9));
                }

                column.optional(isNullable(columnMetadata.getInt(11)));
                column.position(columnMetadata.getInt(17));
//                column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));
//                String autogenerated = null;

//                autogenerated = columnMetadata.getString(24);


//                column.generated("YES".equalsIgnoreCase(autogenerated));
                column.nativeType(this.resolveNativeType(column.typeName()));
                column.jdbcType(this.resolveJdbcType(columnMetadata.getInt(5), column.nativeType()));
                if (defaultValue != null) {
                    this.getDefaultValue(column.create(), defaultValue).ifPresent(column::defaultValue);
                }

                list.add(column.create());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(conn);
            return list;
        }
    }

    public List<String> readPrimaryKeyNames(String tableName) {
        Connection conn = null;
        List<String> list = new ArrayList<>();
        ResultSet rs = null;
        try{
            conn = dbConnector.getConn();
            DatabaseMetaData metadata = conn.getMetaData();
            rs = metadata.getPrimaryKeys("", dataBaseProperty.getUsername().toUpperCase(), tableName);
            while(rs.next()) {
                String columnName = rs.getString(4);
                int columnIndex = rs.getInt(5);
                Collect.set(list, columnIndex - 1, columnName,  null);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(conn);
            return list;
        }
    }

    protected static boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }

    /**
     * Provides a native type for the given type name.
     *
     * There isn't a standard way to obtain this information via JDBC APIs so this method exists to allow
     * database specific information to be set in addition to the JDBC Type.
     *
     * @param typeName the name of the type whose native type we are looking for
     * @return A type constant for the specific database or -1.
     */
    protected int resolveNativeType(String typeName) {
        return Column.UNSET_INT_VALUE;
    }


    /**
     * Resolves the supplied metadata JDBC type to a final JDBC type.
     *
     * @param metadataJdbcType the JDBC type from the underlying driver's metadata lookup
     * @param nativeType the database native type or -1 for unknown
     * @return the resolved JDBC type
     */
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        return metadataJdbcType;
    }

    /**
     * Allow implementations an opportunity to adjust the current state of the {@link ColumnEditor}
     * that has been seeded with data from the column metadata from the JDBC driver.  In some
     * cases, the data from the driver may be misleading and needs some adjustments.
     *
     * @param column the column editor, should not be {@code null}
     * @return the adjusted column editor instance
     */
    protected ColumnEditor overrideColumn(ColumnEditor column) {
        // allows the implementation to override column-specifics; the default does no overrides
        return column;
    }

    protected Optional<Object> getDefaultValue(Column column, String defaultValue) {
        return Optional.empty();
    }
}