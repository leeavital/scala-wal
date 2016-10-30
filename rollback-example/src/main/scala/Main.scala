import java.util.UUID

import scala.collection.mutable

object Main extends App {

  val setOne = mutable.Set[String]()
  val setTwo = mutable.Set[String]()

  def doSet(str: String) = {
    setOne += str

    // fail 10% of writes
    if (Math.random() < 0.1) {
      throw new RuntimeException
    }

    setTwo += str
  }

  def rollback(str: String): Unit = {
    setOne.remove(str)
    setTwo.remove(str)
  }

  val stringCodec = new Codec[String, Array[Byte]] {
    override def serialize(in: String) = in.getBytes
    override def deserialize(bs: Array[Byte]) = new String(bs)
  }

  val writer = new JournalingWriter[String](
      stringCodec, rollback _, FileBasedJournal("test.data"))


  (1 to 1000).par.foreach(n => {
    val str = UUID.randomUUID().toString

    writer.run(str, () => doSet(str))
  })


  println(setOne == setTwo)
}
