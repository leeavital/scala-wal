import java.io.{FileNotFoundException, File}

import com.leeavital.{WALEntry => AvroWalEntry}
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

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
  val lst = collection.mutable.ArrayBuffer.empty[WALEntry]

  override def log(in: WALEntry): Unit = {
    println(s"logging ${in}")
    lst += in
  }

  override def allLogs = {
    lst.toList
  }
}

object FileBasedJournal {

  def apply(filePath: String) = new FileBasedJournal(filePath)
}

class FileBasedJournal(filePath: String) extends Journal {

  val writer = new SpecificDatumWriter[AvroWalEntry](AvroWalEntry.getClassSchema)
  val reader = new SpecificDatumReader[AvroWalEntry](AvroWalEntry.getClassSchema)

  val dataFileWriter = new DataFileWriter[AvroWalEntry](writer)
  val file = new File(filePath)

  if (file.exists()) {
    dataFileWriter.appendTo(file)
  } else {
    dataFileWriter.create(AvroWalEntry.getClassSchema, file)
  }


  val fd = new java.io.File(filePath)

  override def log(entry: WALEntry) = {
    val avro = WALEntryCodec.serialize(entry)
    dataFileWriter.append(avro)
    dataFileWriter.fSync()
  }

  override def allLogs = {
    import scala.collection.JavaConverters._

    try {
      val dataFilereader = DataFileReader.openReader(new File(filePath), reader)
      dataFilereader.iterator().asScala.map(WALEntryCodec.deserialize).toList
    } catch {
      case (e: FileNotFoundException) => {
        println("no file found, assumping first startup")
        List()
      }
    }
  }
}
