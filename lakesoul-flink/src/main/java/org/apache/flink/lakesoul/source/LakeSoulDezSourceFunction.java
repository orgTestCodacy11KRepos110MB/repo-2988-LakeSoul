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

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.Validator;
import com.ververica.cdc.debezium.internal.*;
import com.ververica.cdc.debezium.utils.DatabaseHistoryUtil;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.heartbeat.Heartbeat;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

public class LakeSoulDezSourceFunction<T> extends RichSourceFunction<T> implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<T> {
    
        private static final long serialVersionUID = -5808108641062931623L;
        protected static final Logger LOG = LoggerFactory.getLogger(LakeSoulDezSourceFunction.class);
        public static final String OFFSETS_STATE_NAME = "offset-states";
        public static final String HISTORY_RECORDS_STATE_NAME = "history-records-states";
        public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;
        public static final String LEGACY_IMPLEMENTATION_KEY = "internal.implementation";
        public static final String LEGACY_IMPLEMENTATION_VALUE = "legacy";
        private final DebeziumDeserializationSchema<T> deserializer;
        private final Properties properties;
        @Nullable
        private final DebeziumOffset specificOffset;
        private final LinkedMap pendingOffsetsToCommit = new LinkedMap();
        private volatile boolean debeziumStarted = false;
        private final Validator validator;
        private transient volatile String restoredOffsetState;
        private transient ListState<byte[]> offsetState;
        private transient ListState<String> schemaRecordsState;
        private transient ExecutorService executor;
        private transient DebeziumEngine<?> engine;
        private transient String engineInstanceName;
        private transient DebeziumChangeConsumer changeConsumer;
        private transient DebeziumChangeFetcher<T> debeziumChangeFetcher;
        private transient Handover handover;

    public LakeSoulDezSourceFunction(DebeziumDeserializationSchema<T> deserializer, Properties properties, @Nullable DebeziumOffset specificOffset, Validator validator) {
        this.deserializer = deserializer;
        this.properties = properties;
        this.specificOffset = specificOffset;
        this.validator = validator;
    }

        public void open(Configuration parameters) throws Exception {
        this.validator.validate();
        super.open(parameters);
        ThreadFactory threadFactory = (new ThreadFactoryBuilder()).setNameFormat("debezium-engine").build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.handover = new Handover();
        this.changeConsumer = new DebeziumChangeConsumer(this.handover);
    }

        public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        this.offsetState = stateStore.getUnionListState(new ListStateDescriptor("offset-states", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        this.schemaRecordsState = stateStore.getUnionListState(new ListStateDescriptor("history-records-states", BasicTypeInfo.STRING_TYPE_INFO));
        if (context.isRestored()) {
            this.restoreOffsetState();
            this.restoreHistoryRecordsState();
        } else if (this.specificOffset != null) {
            byte[] serializedOffset = DebeziumOffsetSerializer.INSTANCE.serialize(this.specificOffset);
            this.restoredOffsetState = new String(serializedOffset, StandardCharsets.UTF_8);
            LOG.info("Consumer subtask {} starts to read from specified offset {}.", this.getRuntimeContext().getIndexOfThisSubtask(), this.restoredOffsetState);
        } else {
            LOG.info("Consumer subtask {} has no restore state.", this.getRuntimeContext().getIndexOfThisSubtask());
        }

    }

        private void restoreOffsetState() throws Exception {
        byte[] serializedOffset;
        for(Iterator var1 = ((Iterable)this.offsetState.get()).iterator(); var1.hasNext(); this.restoredOffsetState = new String(serializedOffset, StandardCharsets.UTF_8)) {
            serializedOffset = (byte[])var1.next();
            if (this.restoredOffsetState != null) {
                throw new RuntimeException("Debezium Source only support single task, however, this is restored from multiple tasks.");
            }
        }

        LOG.info("Consumer subtask {} restored offset state: {}.", this.getRuntimeContext().getIndexOfThisSubtask(), this.restoredOffsetState);
    }

        private void restoreHistoryRecordsState() throws Exception {
        DocumentReader reader = DocumentReader.defaultReader();
        ConcurrentLinkedQueue<SchemaRecord> historyRecords = new ConcurrentLinkedQueue();
        int recordsCount = 0;
        boolean firstEntry = true;
        Iterator var5 = ((Iterable)this.schemaRecordsState.get()).iterator();

        while(var5.hasNext()) {
            String record = (String)var5.next();
            if (firstEntry) {
                this.engineInstanceName = record;
                firstEntry = false;
            } else {
                historyRecords.add(new SchemaRecord(reader.read(record)));
                ++recordsCount;
            }
        }

        if (this.engineInstanceName != null) {
            DatabaseHistoryUtil.registerHistory(this.engineInstanceName, historyRecords);
        }

        LOG.info("Consumer subtask {} restored history records state: {} with {} records.", new Object[]{this.getRuntimeContext().getIndexOfThisSubtask(), this.engineInstanceName, recordsCount});
    }

        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (this.handover.hasError()) {
            LOG.debug("snapshotState() called on closed source");
            throw new FlinkRuntimeException("Call snapshotState() on closed source, checkpoint failed.");
        } else {
            this.snapshotOffsetState(functionSnapshotContext.getCheckpointId());
            this.snapshotHistoryRecordsState();
        }
    }

        private void snapshotOffsetState(long checkpointId) throws Exception {
        this.offsetState.clear();
        DebeziumChangeFetcher<?> fetcher = this.debeziumChangeFetcher;
        byte[] serializedOffset = null;
        if (fetcher == null) {
            if (this.restoredOffsetState != null) {
                serializedOffset = this.restoredOffsetState.getBytes(StandardCharsets.UTF_8);
            }
        } else {
            byte[] currentState = fetcher.snapshotCurrentState();
            if (currentState == null && this.restoredOffsetState != null) {
                serializedOffset = this.restoredOffsetState.getBytes(StandardCharsets.UTF_8);
            } else {
                serializedOffset = currentState;
            }
        }

        if (serializedOffset != null) {
            this.offsetState.add(serializedOffset);
            this.pendingOffsetsToCommit.put(checkpointId, serializedOffset);

            while(this.pendingOffsetsToCommit.size() > 100) {
                this.pendingOffsetsToCommit.remove(0);
            }
        }

    }

        private void snapshotHistoryRecordsState() throws Exception {
        this.schemaRecordsState.clear();
        if (this.engineInstanceName != null) {
            this.schemaRecordsState.add(this.engineInstanceName);
            Collection<SchemaRecord> records = DatabaseHistoryUtil.retrieveHistory(this.engineInstanceName);
            DocumentWriter writer = DocumentWriter.defaultWriter();
            Iterator var3 = records.iterator();

            while(var3.hasNext()) {
                SchemaRecord record = (SchemaRecord)var3.next();
                this.schemaRecordsState.add(writer.write(record.toDocument()));
            }
        }

    }

        public void run(SourceContext<T> sourceContext) throws Exception {
        this.properties.setProperty("name", "engine");
        this.properties.setProperty("offset.storage", FlinkOffsetBackingStore.class.getCanonicalName());
        if (this.restoredOffsetState != null) {
            this.properties.setProperty("offset.storage.flink.state.value", this.restoredOffsetState);
        }

        this.properties.setProperty("include.schema.changes", "true");
        this.properties.setProperty("offset.flush.interval.ms", String.valueOf(9223372036854775807L));
        this.properties.setProperty("tombstones.on.delete", "false");
        if (this.engineInstanceName == null) {
            this.engineInstanceName = UUID.randomUUID().toString();
        }

        this.properties.setProperty("database.history.instance.name", this.engineInstanceName);
        this.properties.setProperty("database.history", this.determineDatabase().getCanonicalName());
        String dbzHeartbeatPrefix = this.properties.getProperty(Heartbeat.HEARTBEAT_TOPICS_PREFIX.name(), Heartbeat.HEARTBEAT_TOPICS_PREFIX.defaultValueAsString());
        this.debeziumChangeFetcher = new DebeziumChangeFetcher(sourceContext, this.deserializer, this.restoredOffsetState == null, dbzHeartbeatPrefix, this.handover);
        this.engine = DebeziumEngine.create(Connect.class).using(this.properties).notifying(this.changeConsumer).using(OffsetCommitPolicy.always()).using((success, message, error) -> {
            if (success) {
                this.handover.close();
            } else {
                this.handover.reportError(error);
            }

        }).build();
        this.executor.execute(this.engine);
        this.debeziumStarted = true;
        Method getMetricGroupMethod = this.getRuntimeContext().getClass().getMethod("getMetricGroup");
        getMetricGroupMethod.setAccessible(true);
        MetricGroup metricGroup = (MetricGroup)getMetricGroupMethod.invoke(this.getRuntimeContext());
        metricGroup.gauge("currentFetchEventTimeLag", () -> {
            return this.debeziumChangeFetcher.getFetchDelay();
        });
        metricGroup.gauge("currentEmitEventTimeLag", () -> {
            return this.debeziumChangeFetcher.getEmitDelay();
        });
        metricGroup.gauge("sourceIdleTime", () -> {
            return this.debeziumChangeFetcher.getIdleTime();
        });
        this.debeziumChangeFetcher.runFetchLoop();
    }

        public void notifyCheckpointComplete(long checkpointId) {
        if (!this.debeziumStarted) {
            LOG.debug("notifyCheckpointComplete() called when engine is not started.");
        } else {
            DebeziumChangeFetcher<T> fetcher = this.debeziumChangeFetcher;
            if (fetcher == null) {
                LOG.debug("notifyCheckpointComplete() called on uninitialized source");
            } else {
                try {
                    int posInMap = this.pendingOffsetsToCommit.indexOf(checkpointId);
                    if (posInMap == -1) {
                        LOG.warn("Consumer subtask {} received confirmation for unknown checkpoint id {}", this.getRuntimeContext().getIndexOfThisSubtask(), checkpointId);
                        return;
                    }

                    byte[] serializedOffsets = (byte[])this.pendingOffsetsToCommit.remove(posInMap);

                    for(int i = 0; i < posInMap; ++i) {
                        this.pendingOffsetsToCommit.remove(0);
                    }

                    if (serializedOffsets == null || serializedOffsets.length == 0) {
                        LOG.debug("Consumer subtask {} has empty checkpoint state.", this.getRuntimeContext().getIndexOfThisSubtask());
                        return;
                    }

                    DebeziumOffset offset = DebeziumOffsetSerializer.INSTANCE.deserialize(serializedOffsets);
                    this.changeConsumer.commitOffset(offset);
                } catch (Exception var7) {
                    LOG.warn("Ignore error when committing offset to database.", var7);
                }

            }
        }
    }

        public void cancel() {
        this.shutdownEngine();
        if (this.debeziumChangeFetcher != null) {
            this.debeziumChangeFetcher.close();
        }

    }

        public void close() throws Exception {
        this.cancel();
        if (this.executor != null) {
            this.executor.awaitTermination(9223372036854775807L, TimeUnit.SECONDS);
        }

        super.close();
    }

        private void shutdownEngine() {
        try {
            if (this.engine != null) {
                this.engine.close();
            }
        } catch (IOException var5) {
            ExceptionUtils.rethrow(var5);
        } finally {
            if (this.executor != null) {
                this.executor.shutdownNow();
            }

            this.debeziumStarted = false;
            if (this.handover != null) {
                this.handover.close();
            }

        }

    }

        public TypeInformation<T> getProducedType() {
        return this.deserializer.getProducedType();
    }

        @VisibleForTesting
        public LinkedMap getPendingOffsetsToCommit() {
        return this.pendingOffsetsToCommit;
    }

        @VisibleForTesting
        public boolean getDebeziumStarted() {
        return this.debeziumStarted;
    }

        private Class<?> determineDatabase() {
        boolean isCompatibleWithLegacy = FlinkDatabaseHistory.isCompatible(DatabaseHistoryUtil.retrieveHistory(this.engineInstanceName));
        if ("legacy".equals(this.properties.get("internal.implementation"))) {
            if (isCompatibleWithLegacy) {
                return FlinkDatabaseHistory.class;
            } else {
                throw new IllegalStateException("The configured option 'debezium.internal.implementation' is 'legacy', but the state of source is incompatible with this implementation, you should remove the the option.");
            }
        } else if (FlinkDatabaseSchemaHistory.isCompatible(DatabaseHistoryUtil.retrieveHistory(this.engineInstanceName))) {
            return FlinkDatabaseSchemaHistory.class;
        } else if (isCompatibleWithLegacy) {
            return FlinkDatabaseHistory.class;
        } else {
            throw new IllegalStateException("Can't determine which DatabaseHistory to use.");
        }
    }

        @VisibleForTesting
        public String getEngineInstanceName() {
        return this.engineInstanceName;
    }
    
}
