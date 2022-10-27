/*
 *
 *
 *   Copyright [2022] [DMetaSoul Team]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.flink.lakesoul.source;

import com.ververica.cdc.connectors.oracle.OracleValidator;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import io.debezium.connector.oracle.OracleConnector;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

import static io.debezium.connector.oracle.OracleConnectorConfig.TABLENAME_CASE_INSENSITIVE;

public class OracleSourceBuild<T> {
    private static final String DATABASE_SERVER_NAME = "oracle_logminer";
    private int port = 1521;
    private String hostname;
    private String database;
    private String username;
    private String password;
    private String[] tableList;
    private String[] schemaList;
    private Properties dbzProperties;
    private StartupOptions startupOptions = StartupOptions.initial();
    private DebeziumDeserializationSchema<T> deserializer;

    public OracleSourceBuild() {
    }

    public static <T> OracleSourceBuild<T> builder() {
        return new OracleSourceBuild();
    }

    public OracleSourceBuild<T> hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public OracleSourceBuild<T> port(int port) {
        this.port = port;
        return this;
    }

    public OracleSourceBuild<T> database(String database) {
        this.database = database;
        return this;
    }

    public OracleSourceBuild<T> tableList(String... tableList) {
        this.tableList = tableList;
        return this;
    }

    public OracleSourceBuild<T> schemaList(String... schemaList) {
        this.schemaList = schemaList;
        return this;
    }

    public OracleSourceBuild<T> username(String username) {
        this.username = username;
        return this;
    }

    public OracleSourceBuild<T> password(String password) {
        this.password = password;
        return this;
    }

    public OracleSourceBuild<T> debeziumProperties(Properties properties) {
        this.dbzProperties = properties;
        return this;
    }

    public OracleSourceBuild<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public OracleSourceBuild<T> startupOptions(StartupOptions startupOptions) {
        this.startupOptions = startupOptions;
        return this;
    }

    public LakeSoulDezSourceFunction<T> build() {
        Properties props = new Properties();
        props.setProperty("connector.class", OracleConnector.class.getCanonicalName());
        props.setProperty("database.server.name", (String) Preconditions.checkNotNull(this.database));
        props.setProperty("database.hostname", (String) Preconditions.checkNotNull(this.hostname));
        props.setProperty("database.user", (String) Preconditions.checkNotNull(this.username));
        props.setProperty("database.password", (String) Preconditions.checkNotNull(this.password));
        props.setProperty("database.port", String.valueOf(this.port));
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.dbname", (String) Preconditions.checkNotNull(this.database));
        props.setProperty(TABLENAME_CASE_INSENSITIVE.name(), "false");
        if (this.schemaList != null) {
            props.setProperty("schema.whitelist", String.join(",", this.schemaList));
        }

        if (this.tableList != null) {
            props.setProperty("table.include.list", String.join(",", this.tableList));
        }

        DebeziumOffset specificOffset = null;
        switch (this.startupOptions.startupMode) {
            case INITIAL:
                props.setProperty("snapshot.mode", "initial");
                break;
            case LATEST_OFFSET:
                props.setProperty("snapshot.mode", "schema_only");
                break;
            default:
                throw new UnsupportedOperationException();
        }

        if (this.dbzProperties != null) {
            this.dbzProperties.forEach(props::put);
        }

        return new LakeSoulDezSourceFunction(this.deserializer, props, (DebeziumOffset) specificOffset, new OracleValidator(props));
    }

}
