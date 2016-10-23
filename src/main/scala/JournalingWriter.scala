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
