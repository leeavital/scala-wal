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

trait Journal[T] {
  def log(in: T): Unit

  def allLogs(): List[T]
}

object InMemoryJournal {
  def apply[T](): Journal[T] = {
    val lst = collection.mutable.ArrayBuffer.empty[T]
    new Journal[T] {
      override def log(in: T): Unit = {
        println(s"logging ${in}")
        lst += in
      }

      override def allLogs = {
        lst.toList
      }
    }
  }
}

object JournalingWriter {

  def apply[T](
    codec: Codec[T, Array[Byte]],
    rollback: Function[T, Unit],
    journal: Journal[T]
  ): JournalingWriter[T] = {

    new JournalingWriter[T] {
      override def run[R](log: T, fn: () => R) = {
        journal.log(log)
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

  println(DoSetCodec.deserialize(DoSetCodec.serialize(DoSet("a", 4))))

  val map = collection.mutable.Map.empty[String, Integer]

  val writer = JournalingWriter(
    DoSetCodec,
    (ev: DoSet) => {
      println(s"rolling back ${ev}")
    },
    InMemoryJournal()
  )

  writer.run[Unit](DoSet("a", 2), () => {
    throw new RuntimeException("failed to set a=2")
  })
}
