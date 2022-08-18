package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFileUtil.{getBlockHosts, getBlockLocations, getPartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.merge.{FieldInfo, KeyIndex, MergeDeltaParquetScan, MergeFilePartition, MergePartitionedFile, MergePartitionedFileUtil}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.{LakeSoulFileIndexV2, LakeSoulTableForCdc}
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, TableInfo}
import org.apache.spark.sql.sources.{EqualTo, Filter, Not}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.util.{Locale, OptionalLong}

case class NativeParquetScan(sparkSession: SparkSession,
                             hadoopConf: Configuration,
                             fileIndex: LakeSoulFileIndexV2,
                             dataSchema: StructType,
                             readDataSchema: StructType,
                             readPartitionSchema: StructType,
                             pushedFilters: Array[Filter],
                             options: CaseInsensitiveStringMap,
                             tableInfo: TableInfo,
                             partitionFilters: Seq[Expression] = Seq.empty,
                             dataFilters: Seq[Expression] = Seq.empty
                              )
  extends MergeDeltaParquetScan(sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    options,
    tableInfo,
    partitionFilters,
    dataFilters) {


  override def createReaderFactory(): PartitionReaderFactory = {
    logInfo("[Debug][huazeng]on org.apache.spark.sql.execution.datasources.v2.parquet.NativeParquetScan.createReaderFactory")
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))


    NativeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
  }

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }

  override lazy val newFileIndex: LakeSoulFileIndexV2 = fileIndex

  override protected def partitions: Seq[MergeFilePartition] = {
    val selectedPartitions = newFileIndex.listFiles(partitionFilters, dataFilters)
    val partitionAttributes = newFileIndex.partitionSchema.toAttributes
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw new AnalysisException(s"Can't find required partition column ${readField.name} " +
          s"in partition schema ${newFileIndex.partitionSchema}")
      )
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val splitFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }

      // produce requested schema
      val requestedFields = readDataSchema.fieldNames
      val requestFilesSchemaMap = fileInfo
        .groupBy(_.range_version)
        .map(m => {
          val fileExistCols = m._2.head.file_exist_cols.split(",")
          (m._1, StructType(
            requestedFields.filter(f => fileExistCols.contains(f) || tableInfo.hash_partition_columns.contains(f))
              .map(c => tableInfo.schema(c))
          ))
        })

      partition.files.flatMap { file =>
        val filePath = file.getPath

        Seq(getPartitionedFile(
          sparkSession,
          file,
          filePath,
          partitionValues,
          tableInfo,
          fileInfo,
          requestFilesSchemaMap,
          readDataSchema,
          readPartitionSchema.fieldNames))
      }.toSeq
      //.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    if (splitFiles.length == 1) {
      val path = new Path(splitFiles(0).filePath)
      if (!isSplittable(path) && splitFiles(0).length >
        sparkSession.sparkContext.getConf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
      }
    }

    //    MergeFilePartition.getFilePartitions(sparkSession.sessionState.conf, splitFiles, tableInfo.bucket_num)
    getFilePartitions(sparkSession.sessionState.conf, splitFiles, tableInfo.bucket_num)
  }

  override def getFilePartitions(conf: SQLConf,
                                 partitionedFiles: Seq[MergePartitionedFile],
                                 bucketNum: Int): Seq[MergeFilePartition] = {
    logInfo("[Debug][huazeng]on org.apache.spark.sql.execution.datasources.v2.parquet.NativeParquetScan.getFilePartitions")
    val fileWithBucketId: Map[Int, Map[String, Seq[MergePartitionedFile]]] = partitionedFiles
      .groupBy(_.fileBucketId)
      .map(f => (f._1, f._2.groupBy(_.rangeKey)))

    Seq.tabulate(bucketNum) { bucketId =>
      val files = fileWithBucketId.getOrElse(bucketId, Map.empty[String, Seq[MergePartitionedFile]])
        .map(_._2.toArray).toArray

      var isSingleFile = false
      for (index <- 0 to files.size - 1) {
        isSingleFile = files(index).size == 1
        if (!isSingleFile) {
          val versionFiles = for (elem <- 0 to files(index).size - 1) yield files(index)(elem).copy(writeVersion = elem)
          files(index) = versionFiles.toArray
        }
      }
      MergeFilePartition(bucketId, files, isSingleFile)
    }
  }


  def getPartitionedFile(sparkSession: SparkSession,
                         file: FileStatus,
                         filePath: Path,
                         partitionValues: InternalRow,
                         tableInfo: TableInfo,
                         fileInfo: Seq[DataFileInfo],
                         requestFilesSchemaMap: Map[String, StructType],
                         requestDataSchema: StructType,
                         requestPartitionFields: Array[String]): MergePartitionedFile = {
    val hosts = getBlockHosts(getBlockLocations(file), 0, file.getLen)

    val filePathStr = filePath
      .getFileSystem(sparkSession.sessionState.newHadoopConf())
      .makeQualified(filePath).toString
    val touchedFileInfo = fileInfo.find(f => filePathStr.equals(f.path))
      .getOrElse(throw LakeSoulErrors.filePathNotFoundException(filePathStr, fileInfo.mkString(",")))

    val touchedFileSchema = requestFilesSchemaMap(touchedFileInfo.range_version).fieldNames

    val keyInfo = tableInfo.hash_partition_schema.map(f => {
      KeyIndex(touchedFileSchema.indexOf(f.name), f.dataType)
    })
    val fileSchemaInfo = requestFilesSchemaMap(touchedFileInfo.range_version).map(m => (m.name, m.dataType))
    val partitionSchemaInfo = requestPartitionFields.map(m => (m, tableInfo.range_partition_schema(m).dataType))
    val requestDataInfo = requestDataSchema.map(m => (m.name, m.dataType))

    MergePartitionedFile(
      partitionValues = partitionValues,
      filePath = filePath.toUri.toString,
      start = 0,
      length = file.getLen,
      qualifiedName = filePathStr,
      rangeKey = touchedFileInfo.range_partitions,
      keyInfo = keyInfo,
      resultSchema = (requestDataInfo ++ partitionSchemaInfo).map(m => FieldInfo(m._1, m._2)),
      fileInfo = (fileSchemaInfo ++ partitionSchemaInfo).map(m => FieldInfo(m._1, m._2)),
      writeVersion = 1,
      rangeVersion = touchedFileInfo.range_version,
      fileBucketId = -1,
      locations = hosts)
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(blockLocations: Array[BlockLocation],
                            offset: Long,
                            length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block. It handles the case where the
      // fragment is fully contained in the block.
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if b.getOffset < offset + length && offset + length < b.getOffset + b.getLength =>
        b.getHosts -> (offset + length - b.getOffset)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }
}