sealed trait WALEntry

case class Begin(txId: Long, data: Array[Byte]) extends WALEntry

case class Commit(txId: Long) extends WALEntry

case class Rollback(txId: Long) extends WALEntry


object WALEntryCodec extends Codec[WALEntry, Array[Byte]] {
  override def serialize(in: WALEntry): Array[Byte] = {
    in match {
      case Begin(id, bs) => ("b:" + id + ":" + new String(bs)).getBytes
      case Commit(id) => ("c:" + id).getBytes
      case Rollback(id) => ("r:" + id).getBytes()
    }
  }

  override def deserialize(in: Array[Byte]): WALEntry = {
    val str = new String(in)
    str.split(":") match {
      case Array("c", id) => Commit(id.toLong)
      case toks @ Array("b", id, _*) => Begin(id.toLong, toks.toList.drop(2).reduce(_ + ":" + _).getBytes)
      case Array("r", id) => Rollback(id.toLong)
    }
  }
}
