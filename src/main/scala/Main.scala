import java.io.{FileOutputStream}
import java.nio.charset.Charset

case class DoSet(key: String, value: Integer)

object DoSetCodec extends Codec[DoSet, Array[Byte]] {
  def serialize(set: DoSet) = {
    s"${set.key}:${set.value}".getBytes
  }

  def deserialize(bs: Array[Byte]) = {
    val segs = new String(bs).split(":")
    DoSet(segs(0),  Integer.parseInt(segs(1)))
  }
}


trait Codec[I, O] {
  def serialize(in: I): O

  def deserialize(in: O): I
}

/**
 * A journalling writer which journals events of type T.
 */
trait JournalingWriter[T] {
  /**
   * run an event that may partially fail. The event should be totally replayable
   * from the first parameter, <code>log</code>.
   *
   * The action will not be run until the log is written.
   */
  def run[R](log: T, action: () => R): Option[R]
}

trait Journal {
  type Bytes = Array[Byte]
  def log(in: Bytes): Unit

  def allLogs(): List[Bytes]
}

object InMemoryJournal {
  def apply(): Journal = {
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

object JournalingWriter {

  def apply[T](
    codec: Codec[T, Array[Byte]],
    rollback: Function[T, Unit],
    journal: Journal
  ): JournalingWriter[T] = {

    new JournalingWriter[T] {
      override def run[R](log: T, fn: () => R) = {
        val bs = codec.serialize(log)
        journal.log(bs)
        try {
          Option(fn())
        } catch {
          case (e: Throwable) => {
            println(s"caught '${e.getMessage}', rolling back?")
            rollback(log)
            None
          }
        }
      }
    }
  }
}

object Main extends App {
  println("Hello world")

  val map = collection.mutable.Map.empty[String, Integer]

  val writer = JournalingWriter(
    DoSetCodec,
    (ev: DoSet) => {
      println(s"rolling back ${ev}")
    },
    FileBasedJournal("wal.log")
  )

  writer.run[Unit](DoSet("a", 2), () => {
    throw new RuntimeException("failed to set a=2")
  })
}
