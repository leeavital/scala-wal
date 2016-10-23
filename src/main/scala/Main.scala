

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
