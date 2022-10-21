package com.dmetasoul.lakesoul.meta.external.oracle;

import com.dmetasoul.lakesoul.meta.external.jdbc.JdbcDataTypeConverter;
import io.debezium.relational.Column;
import org.apache.spark.sql.types.DataType;

class OracleDataTypeConverter extends JdbcDataTypeConverter {
    public OracleDataTypeConverter() {
        super();
    }

    public DataType schemaBuilder(Column column) {
        return super.schemaBuilder(column);
    }
}
