/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta._
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.lakesoul.exception.MetaRerunException
import org.apache.spark.sql.lakesoul.schema.SchemaUtils
import org.apache.spark.sql.lakesoul.utils._

import java.util.{ConcurrentModificationException, UUID}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TransactionCommit(override val snapshotManagement: SnapshotManagement) extends Transaction {

}

object TransactionCommit {
  private val active = new ThreadLocal[TransactionCommit]

  /** Get the active transaction */
  def getActive: Option[TransactionCommit] = Option(active.get())

  /**
    * Sets a transaction as the active transaction.
    *
    * @note This is not meant for being called directly, only from
    *       `OptimisticTransaction.withNewTransaction`. Use that to create and set active tc.
    */
  private[lakesoul] def setActive(tc: TransactionCommit): Unit = {
    if (active.get != null) {
      throw new IllegalStateException("Cannot set a new TransactionCommit as active when one is already active")
    }
    active.set(tc)
  }

  /**
    * Clears the active transaction as the active transaction.
    *
    * @note This is not meant for being called directly, `OptimisticTransaction.withNewTransaction`.
    */
  private[lakesoul] def clearActive(): Unit = {
    active.set(null)
  }
}

class PartMergeTransactionCommit(override val snapshotManagement: SnapshotManagement) extends Transaction {

}

object PartMergeTransactionCommit {
  private val active = new ThreadLocal[PartMergeTransactionCommit]

  /** Get the active transaction */
  def getActive: Option[PartMergeTransactionCommit] = Option(active.get())

  /**
    * Sets a transaction as the active transaction.
    *
    * @note This is not meant for being called directly, only from
    *       `OptimisticTransaction.withNewTransaction`. Use that to create and set active tc.
    */
  private[lakesoul] def setActive(tc: PartMergeTransactionCommit): Unit = {
    if (active.get != null) {
      throw new IllegalStateException("Cannot set a new TransactionCommit as active when one is already active")
    }
    active.set(tc)
  }

  /**
    * Clears the active transaction as the active transaction.
    *
    * @note This is not meant for being called directly, `OptimisticTransaction.withNewTransaction`.
    */
  private[lakesoul] def clearActive(): Unit = {
    active.set(null)
  }
}

trait Transaction extends TransactionalWrite with Logging {
  val snapshotManagement: SnapshotManagement

  def snapshot: Snapshot = snapshotManagement.snapshot

  //todo
  private val spark = SparkSession.active

  /** Whether this commit is delta commit */
  override protected var commitType: Option[CommitType] = None

  /** Whether this table has a short table name */
  override protected var shortTableName: Option[String] = None

  def setCommitType(value: String): Unit = {
    assert(commitType.isEmpty, "Cannot set commit type more than once in a transaction.")
    commitType = Some(CommitType(value))
  }

  def setShortTableName(value: String): Unit = {
    assert(!new Path(value).isAbsolute, s"Short Table name `$value` can't be a path")
    assert(shortTableName.isEmpty, "Cannot set short table name more than once in a transaction.")
    assert(tableInfo.short_table_name.isEmpty || tableInfo.short_table_name.get.equals(value),
      s"Table `$table_name` already has a short name `${tableInfo.short_table_name.get}`, " +
        s"you can't change it to `$value`")
    if (tableInfo.short_table_name.isEmpty) {
      shortTableName = Some(value)
    }
  }

  def isFirstCommit: Boolean = snapshot.isFirstCommit

  protected lazy val table_name: String = tableInfo.table_name.get

  /**
    * Tracks the data that could have been seen by recording the partition
    * predicates by which files have been queried by by this transaction.
    */
  protected val readPredicates = new ArrayBuffer[Expression]

  /** Tracks specific files that have been seen by this transaction. */
  protected val readFiles = new mutable.HashSet[DataFileInfo]

  /** Tracks if this transaction has already committed. */
  protected var committed = false

  /** Stores the updated TableInfo (if any) that will result from this tc. */
  protected var newTableInfo: Option[TableInfo] = None

  /** For new tables, fetch global configs as TableInfo. */
  private val snapshotTableInfo: TableInfo = if (snapshot.isFirstCommit) {
    val updatedConfig = LakeSoulConfig.mergeGlobalConfigs(
      spark.sessionState.conf, Map.empty)
    TableInfo(
      table_name = Some(snapshot.getTableName),
      table_id = snapshot.getTableInfo.table_id,
      configuration = updatedConfig)
  } else {
    snapshot.getTableInfo
  }

  /** Returns the TableInfo at the current point in the log. */
  def tableInfo: TableInfo = newTableInfo.getOrElse(snapshotTableInfo)

  /** Set read files when compacting part of table files. */
  def setReadFiles(files: Seq[DataFileInfo]): Unit = {
    readFiles.clear()
    readFiles ++= files
  }

  /**
    * Records an update to the TableInfo that should be committed with this transaction.
    * Note that this must be done before writing out any files so that file writing
    * and checks happen with the final TableInfo for the table.
    *
    * IMPORTANT: It is the responsibility of the caller to ensure that files currently
    * present in the table are still valid under the new TableInfo.
    */
  def updateTableInfo(table_info: TableInfo): Unit = {
    assert(!hasWritten,
      "Cannot update the metadata in a transaction that has already written data.")
    assert(newTableInfo.isEmpty,
      "Cannot change the metadata more than once in a transaction.")

    val updatedTableInfo = if (isFirstCommit) {
      val updatedConfigs = LakeSoulConfig.mergeGlobalConfigs(
        spark.sessionState.conf, table_info.configuration)
      table_info.copy(
        configuration = updatedConfigs)
    } else{
      table_info
    }
    // if task run maybe inconsistent with new update tableinfo; no two phase protocol
    verifyNewMetadata(updatedTableInfo)
    newTableInfo = Some(updatedTableInfo)
  }

  protected def verifyNewMetadata(table_info: TableInfo): Unit = {
    SchemaUtils.checkColumnNameDuplication(table_info.schema, "in the TableInfo update")
    ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(table_info.data_schema))
  }


  /** Returns files matching the given predicates. */
  def filterFiles(): Seq[DataFileInfo] = filterFiles(Seq(Literal.apply(true)))

  /** Returns files matching the given predicates. */
  //Only filtered by partition, dataFilter has not been used
  def filterFiles(filters: Seq[Expression]): Seq[DataFileInfo] = {
    val scan = PartitionFilter.filesForScan(snapshot, filters)
    val partitionFilters = filters.filter { f =>
      LakeSoulUtils.isPredicatePartitionColumnsOnly(f, tableInfo.range_partition_columns, spark)
    }
    readPredicates += partitionFilters.reduceLeftOption(And).getOrElse(Literal(true))
    readFiles ++= scan
    scan
  }

  def getCompactionPartitionFiles(partitionInfo: PartitionInfo): Seq[DataFileInfo] = {
    val files = DataOperation.getSinglePartitionDataInfo(partitionInfo)

    readFiles ++= files
    files
  }

  def commit(addFiles: Seq[DataFileInfo],
             expireFiles: Seq[DataFileInfo],
             newTableInfo: TableInfo): Unit = {
    updateTableInfo(newTableInfo)
    commit(addFiles, expireFiles)
  }


  @throws(classOf[ConcurrentModificationException])
  def commit(addFiles: Seq[DataFileInfo],
             expireFiles: Seq[DataFileInfo],
             query_id: String = "", //for streaming commit
             batch_id: Long = -1L): Unit = {
    snapshotManagement.lockInterruptibly {
      assert(!committed, "Transaction already committed.")
      if (isFirstCommit) {
        MetaVersion.createNewTable(
          table_name,
          tableInfo.table_id,
          tableInfo.table_schema,
          tableInfo.range_column,
          tableInfo.hash_column,
          tableInfo.configuration,
          tableInfo.bucket_num
        )
      }

      val depend_files = readFiles.toSeq ++ addFiles ++ expireFiles

      //Gets all the partition names that need to be changed
      val depend_partitions = depend_files
        .groupBy(_.range_partitions).keys
//        .map(getPartitionKeyFromMap)
        .toSet

      val add_file_arr_buf = new ArrayBuffer[DataCommitInfo]()
      val expire_file_arr_buf = new ArrayBuffer[DataCommitInfo]()

      val add_partition_info_arr_buf = new ArrayBuffer[PartitionInfo]()
      val expire_partition_info_arr_buf = new ArrayBuffer[PartitionInfo]()

      for (range_key <- depend_partitions) {
        val add_file_arr_buf_tmp = new ArrayBuffer[DataFileInfo]()
        for (file <- addFiles) {
          if (file.range_partitions.equalsIgnoreCase(range_key)) {
            add_file_arr_buf_tmp += file
          }
        }
        val addUUID = UUID.randomUUID()
        add_file_arr_buf += DataCommitInfo(
          tableInfo.table_id,
          range_key,
          addUUID,
          commitType.getOrElse(CommitType("append")).name,
          //todo
          System.currentTimeMillis(),
          add_file_arr_buf_tmp.toArray
        )
        add_partition_info_arr_buf += PartitionInfo(
          table_id = tableInfo.table_id,
          range_value = range_key,
          read_files = Array(addUUID)
        )
      }

      val meta_info = MetaInfo(
        table_info = tableInfo,
        dataCommitInfo = add_file_arr_buf.toArray,
        partitionInfoArray = add_partition_info_arr_buf.toArray,
        commit_type = commitType.getOrElse(CommitType("append"))
      )

      try {
        val changeSchema = !isFirstCommit && newTableInfo.nonEmpty
        MetaCommit.doMetaCommit(meta_info, changeSchema)
      } catch {
        case e: MetaRerunException => throw e
        case e: Throwable => throw e
      }

      committed = true
    }
    snapshotManagement.updateSnapshot()
  }
}
