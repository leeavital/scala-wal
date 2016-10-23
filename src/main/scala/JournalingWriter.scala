import scala.util.Random

class JournalingWriter[T](
                           codec: Codec[T, Array[Byte]],
                           rollback: Function[T, Unit],
                           journal: Journal) {

  val seed = new Random()



  /**
    * run an event that may partially fail. The event should be totally replayable
    * from the first parameter, <code>log</code>.
    *
    * The action will not be run until the log is written.
    */
  def run[R](log: T, fn: () => R) = {
    val txId = seed.nextLong()

    val data = codec.serialize(log)
    val startTx = Begin(txId, data)

    journal.log(WALEntryCodec.serialize(startTx))
    try {
      Option(fn())
      journal.log(WALEntryCodec.serialize(Commit(txId)))
    } catch {
      case (e: Throwable) => {
        println(s"caught '${e.getMessage}', rolling back?")
        rollback(log)
        journal.log(WALEntryCodec.serialize(Rollback(txId)))
        None
      }
    }
  }

  /**
    * Read the log and rollback any uncommited and unrolled back transactions. This should be called on startup.
    */
  def repair = {
    val entries = journal.allLogs().toList.map(WALEntryCodec.deserialize)

    val finishedTransactions = entries.flatMap {
      case Commit(tx) => Some(tx)
      case Rollback(tx) => Some(tx)
      case Begin(tx, _) => None
    }

    val unendedBegins = entries.flatMap {
      case Commit(tx) => None
      case Rollback(tx) => None
      case begin @ Begin(tx, _) =>
        if (finishedTransactions.contains(tx)) {
          None
        } else {
          Some(begin)
        }
    }

    unendedBegins.map(begin => {
      val data = codec.deserialize(begin.data)
      println(s"rolling back ${begin.txId}")
      rollback(data)
      journal.log(WALEntryCodec.serialize(Rollback(begin.txId)))
    })
  }
}
