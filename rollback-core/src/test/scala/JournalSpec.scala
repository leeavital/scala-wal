import java.io.File
import java.nio.charset.Charset
import java.util.UUID

/**
  * Created by lee on 10/29/16.
  */
class JournalSpec extends Spec {

  val bytesOne = "one".getBytes(Charset.defaultCharset())
  val bytesTwo = "two".getBytes(Charset.defaultCharset())


  val file = System.getProperty("java.io.tmpdir") + s"temp-wal-${UUID.randomUUID().toString}"

  lazy val log = FileBasedJournal(file)

  after {
    new File(file).delete()
  }

  "JournalSpec" - {
    "after logging" - {
      log.log(Begin(1L, bytesOne))
      log.log(Begin(2L, bytesTwo))

      log.log(Commit(1L))

      val allLogs = FileBasedJournal(file).allLogs

      "should have the correct number of entries" in {
        allLogs should have size(3)
      }

      "should contain entries in the correct order" in {

        allLogs(0) should beBegin(1L)
        allLogs(1) should beBegin(2L)
        allLogs(2) should be(Commit(1L))
      }
    }

    "not crash on the same two datums" in {
      log.log(Begin(1L, bytesOne))
      log.log(Begin(1L, bytesOne))
    }
  }
}
