package org.apache.flink.lakesoul.test;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.relational.Tables;

public class OracleDdlParserTest {
    public static void main(String[] args) {
        OracleDdlParser parser = new OracleDdlParser();
        String ddl;
        ddl = "CREATE TABLE countries_demo\n" +
                "    ( country_id      CHAR(2)\n" +
                "      CONSTRAINT country_id_nn_demo NOT NULL\n" +
                "    , country_name    VARCHAR2(40)\n" +
                "    , currency_name   VARCHAR2(25)\n" +
                "    , currency_symbol VARCHAR2(3)\n" +
                "    , region          VARCHAR2(15)\n" +
                "    , CONSTRAINT    country_c_id_pk_demo\n" +
                "                    PRIMARY KEY (country_id ) )\n" +
                "    ORGANIZATION INDEX \n" +
                "    INCLUDING   country_name \n" +
                "    PCTTHRESHOLD 2 \n" +
                "    STORAGE \n" +
                "     ( INITIAL  4K ) \n" +
                "   OVERFLOW \n" +
                "    STORAGE \n" +
                "      ( INITIAL  4K );";
        parser.parse(ddl, new Tables());
    }
}
