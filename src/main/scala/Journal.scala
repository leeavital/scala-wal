import java.io.File

import com.leeavital.{WALEntry => AvroWalEntry}
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.slf4j.LoggerFactory

trait Journal {
  def log(in: WALEntry): Unit

  def allLogs(): List[WALEntry]
}

object InMemoryJournal {
  def apply() = {
    new InMemoryJournal
  }
}

class InMemoryJournal extends Journal {
  val log = LoggerFactory.getLogger(this.getClass)

  val lst = collection.mutable.ArrayBuffer.empty[WALEntry]

  override def log(in: WALEntry): Unit = {
    lst += in
  }

  override def allLogs = {
    lst.toList
  }
}

object FileBasedJournal {

  def apply(filePath: String) = {
    new FileBasedJournal(filePath)
  }
}

class FileBasedJournal(filePath: String) extends Journal {

  val log = LoggerFactory.getLogger(this.getClass)
  val eventLog = LoggerFactory.getLogger("wal.event")

  val writer = new SpecificDatumWriter[AvroWalEntry](AvroWalEntry.getClassSchema)
  val reader = new SpecificDatumReader[AvroWalEntry](AvroWalEntry.getClassSchema)

  val dataFileWriter = new DataFileWriter[AvroWalEntry](writer)
  val file = new File(filePath)

  if (file.exists()) {
    dataFileWriter.appendTo(file)
  } else {
    dataFileWriter.create(AvroWalEntry.getClassSchema, file)
  }

  override def log(entry: WALEntry) = {
    if (eventLog.isDebugEnabled) {
      eventLog.debug("will log: {}", entry)
    }
    val avroEntry = WALEntryCodec.serialize(entry)
    dataFileWriter.append(avroEntry)
    dataFileWriter.fSync()
  }

  override def allLogs = {
    import scala.collection.JavaConverters._

    if (file.exists()) {
      val dataFileReader = DataFileReader.openReader(new File(filePath), reader)
      dataFileReader.iterator().asScala.map(WALEntryCodec.deserialize).toList
    } else {
      log.info("no file found, assuming first startup")
      List()
    }
  }
}
