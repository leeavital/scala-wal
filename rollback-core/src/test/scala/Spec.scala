import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.{MatchResult, Matcher}

@RunWith(classOf[JUnitRunner])
abstract class Spec extends FreeSpec with Matchers with BeforeAndAfter with BeforeAndAfterEach {
  def beBegin(i: Long) = {
    new Matcher[WALEntry] {
      override def apply(left: WALEntry): MatchResult = {
        val failure =  s"${left} was did not match Begin(${i})"
        val success = s"${left} matched Begin(${i})"

        val matched = left match {
          case Begin(id, _) => id == i
          case _ => false
        }
        MatchResult(matched, failure, success)
      }
    }
  }
}
