import java.nio.charset.Charset

import scala.collection.mutable

/**
  * Created by lee on 10/30/16.
  */
class JournalingWriterSpec extends Spec {

  val stringCodec = new Codec[String, Array[Byte]] {
    override def serialize(in: String): Array[Byte] = in.getBytes(Charset.defaultCharset())

    override def deserialize(in: Array[Byte]): String = new String(in)
  }

  def fixtures = {
    val rollbacks = mutable.Set[String]()
    val doRollback: String => Unit = (x: String) => rollbacks += x

    val journal = InMemoryJournal()
    val writer = new JournalingWriter[String](stringCodec, doRollback, journal)

    (writer, rollbacks, journal)
  }

  "writer" - {

    "upon operations succeeding" - {
      val (writer, rollbacks, journal) = fixtures

      val result = writer.run("hello", () => {
        println("no op")
      })

      "should log the correct number of things" in {
        journal.allLogs should have size (2)
      }

      "should log the correct things" in {

        journal.allLogs match {
          case List(Begin(id, bs), Commit(id2)) =>
            id should equal(id2)
            stringCodec.deserialize(bs) should be("hello")

          case _ => fail(s"allLogs were wrong: s${journal.allLogs}")
        }
      }

      "should return the right thing" in {
         result should be(true)
      }

      "should not rollback anything" in {
        rollbacks shouldBe empty
      }
    }

    "upon operations failing" - {
      val (writer, rollbacks, journal) = fixtures

      val result = writer.run("goodbye", () => {
        throw new RuntimeException()
      })

      "should rollback the right thing" in {
        journal.allLogs match {
          case List(Begin(id, bs), Rollback(id2)) =>
            id should equal(id2)
            stringCodec.deserialize(bs) should be("goodbye")

            rollbacks should be(Set("goodbye"))
        }
      }

      "should return the right thing" in {
          result should be(false)
      }
    }
  }

  "repair" - {
    "should do nothing when the log is empty" in {
      val (writer, rollbacks, journal) = fixtures

      writer.repair

      rollbacks shouldBe empty
    }


    "should rollback uncommited Begins" in {
      val (writer, rollbacks, journal) = fixtures

      journal.log(Begin(1L, "hello".getBytes(Charset.defaultCharset())))

      writer.repair

      rollbacks should be(Set("hello"))
    }
  }
}
