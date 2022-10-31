package com.dmetasoul.lakesoul.meta.external.oracle;

import com.dmetasoul.lakesoul.meta.external.jdbc.JdbcDataTypeConverter;
import io.debezium.relational.Column;
import org.apache.spark.sql.types.DataType;

import java.sql.Types;

import static java.util.regex.Pattern.matches;
import static org.apache.spark.sql.types.DataTypes.*;

class OracleDataTypeConverter extends JdbcDataTypeConverter {
    public OracleDataTypeConverter() {
        super();
    }

    public DataType schemaBuilder(Column column) {
        // Handle a few ORACLE-specific types based upon how they are handled by the MySQL binlog client ...
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "BINARY_FLOAT")) {
            return FloatType;
        }
        if (matches(typeName, "BINARY_DOUBLE")) {
            return DoubleType;
        }
        if (matches(typeName, "DOUBLE PRECISION")) {
            return DoubleType;
        }
        if (matches(typeName, "FLOAT")) {
            return DoubleType;
        }
        if (matches(typeName, "REAL")) {
            return DoubleType;
        }
        if (matches(typeName, "NUMBER")) {
            if (column.length()==0 && column.scale().get()==0) {
                return DoubleType;
            }
        }
        return super.schemaBuilder(column);
    }
}
