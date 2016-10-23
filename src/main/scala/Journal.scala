import java.io.FileOutputStream
import java.nio.charset.Charset

trait Journal {
  type Bytes = Array[Byte]
  def log(in: Bytes): Unit

  def allLogs(): List[Bytes]
}

object InMemoryJournal {
  def apply(): Journal = {
    println("creating InMemoryJournal")
    val lst = collection.mutable.ArrayBuffer.empty[Array[Byte]]
    new Journal {
      override def log(in: Array[Byte]): Unit = {
        println(s"logging ${new String(in)}")
        lst += in
      }

      override def allLogs = {
        lst.toList
      }
    }
  }
}

object FileBasedJournal {
  def apply(filePath: String): Journal = {
    val fd =  new java.io.File(filePath)
    val os = new FileOutputStream(fd)

    new Journal {
      override def log(bs: Array[Byte]) = {
        os.write(bs)
        os.write("\n".getBytes)
      }

      override def allLogs = {
        val source = io.Source.fromFile(fd, Charset.defaultCharset().toString)
        val lines : Iterator[Array[Byte]] = source.getLines().map(string => string.getBytes)
        lines.toList
      }
    }
  }
}
