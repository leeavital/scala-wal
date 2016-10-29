import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.leeavital.{EntryType, WALEntry => AvroWalEntry}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

sealed trait WALEntry

case class Begin(txId: Long, data: Array[Byte]) extends WALEntry

case class Commit(txId: Long) extends WALEntry

case class Rollback(txId: Long) extends WALEntry


object WALEntryCodec extends Codec[WALEntry, Array[Byte]] {

  val writer = new SpecificDatumWriter[AvroWalEntry](AvroWalEntry.getClassSchema)
  val reader = new SpecificDatumReader[AvroWalEntry](AvroWalEntry.getClassSchema)

  override def serialize(in: WALEntry): Array[Byte] = {
    val asAvro = in match {
      case Begin(id, bs) => AvroWalEntry.newBuilder().setData(ByteBuffer.wrap(bs)).setTxId(id).setType(EntryType.Begin).build()
      case Commit(id) => AvroWalEntry.newBuilder().setType(EntryType.Commit).setTxId(id).build()
      case Rollback(id) => AvroWalEntry.newBuilder().setTxId(id).setType(EntryType.Rollback).build()
    }

    val stream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(stream, null)
    writer.write(asAvro, encoder)
    encoder.flush()
    stream.close()
    stream.toByteArray
  }

  override def deserialize(in: Array[Byte]): WALEntry = {

    val decoder = DecoderFactory.get().binaryDecoder(in, null)
    val entry = reader.read(null, decoder)
    entry.getType match {
      case EntryType.Commit => Commit(entry.getTxId)
      case EntryType.Rollback => Rollback(entry.getTxId)
      case EntryType.Begin => Begin(entry.getTxId, entry.getData.array())
    }
  }
}


object Foo extends  App {

  val bs = WALEntryCodec.serialize(Commit(5L))

  println(new String(bs))

  val entry = WALEntryCodec.deserialize(bs)

}
