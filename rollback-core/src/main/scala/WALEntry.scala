import java.nio.ByteBuffer

import com.leeavital.{EntryType, WALEntry => AvroWalEntry}

sealed trait WALEntry

case class Begin(txId: Long, data: Array[Byte]) extends WALEntry

case class Commit(txId: Long) extends WALEntry

case class Rollback(txId: Long) extends WALEntry


object WALEntryCodec extends Codec[WALEntry, AvroWalEntry] {

  override def serialize(in: WALEntry) =  {
    in match {
      case Begin(id, bs) => AvroWalEntry.newBuilder().setData(ByteBuffer.wrap(bs)).setTxId(id).setType(EntryType.Begin).build()
      case Commit(id) => AvroWalEntry.newBuilder().setType(EntryType.Commit).setTxId(id).build()
      case Rollback(id) => AvroWalEntry.newBuilder().setTxId(id).setType(EntryType.Rollback).build()
    }
  }


  override def deserialize(entry: AvroWalEntry): WALEntry = {
    entry.getType match {
      case EntryType.Commit => Commit(entry.getTxId)
      case EntryType.Rollback => Rollback(entry.getTxId)
      case EntryType.Begin => Begin(entry.getTxId, entry.getData.array())
    }
  }
}
