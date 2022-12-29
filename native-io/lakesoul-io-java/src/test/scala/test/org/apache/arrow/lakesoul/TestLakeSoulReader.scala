package test.org.apache.arrow.lakesoul

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.lakesoul.io.NativeIOWrapper
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader
import org.apache.arrow.lakesoul.memory.ArrowMemoryUtils
import org.apache.arrow.vector.VectorSchemaRoot

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.DurationInt
import scala.util.Success

case class TestLakeSoulReader() extends org.scalatest.funsuite.AnyFunSuite with org.scalatest.BeforeAndAfterAll with org.scalatest.BeforeAndAfterEach{
    val projectDir = "/path/to/project/"

    test("test ArrowCDataWrapper constructor with single file") {

        val wrapper = new NativeIOWrapper()
        wrapper.initialize()
        val filePath = projectDir + "native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
        wrapper.addFile(filePath);
        wrapper.setThreadNum(2)
        wrapper.createReader()
        wrapper.startReader(_=>{})

        val allocator =
            ArrowMemoryUtils.rootAllocator.newChildAllocator("testArrowCDataWrapper", 0, Long.MaxValue)
        val arrowSchema  = ArrowSchema.allocateNew(allocator)
        val arrowArray = ArrowArray.allocateNew(allocator)
        val provider = new CDataDictionaryProvider()
        val schemaPtr: Long = arrowSchema.memoryAddress()
        val arrayPtr: Long = arrowArray.memoryAddress()
        wrapper.nextBatch((hasNext) => {
            println("[From Java]In wrapper.nextBatch() closure:")
            println("[From Java]","hasNext", hasNext)
            if (hasNext) {

                val vsr = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, provider)
                println(vsr.contentToTSVString())
            } else {
                println(hasNext)
            }
        }, schemaPtr, arrayPtr)

        // wait for wrapper.nextBatch() closure finished
        Thread.sleep(2000)



        wrapper.close()
    }

}