import scala.slick.driver.PostgresDriver.simple._
import Database.threadLocalSession
import com.github.tototoshi.slick.JodaSupport._
import org.joda.time.DateTime
import scala.slick.driver.PostgresDriver
import com.github.tminglei.slickpg._
import com.github.tototoshi.csv._
// import grizzled.slf4j.Logger

trait PostgresSupport {
  def db = Database.forURL(
    url    = "jdbc:postgresql://localhost:5432/pdx-scala",
    driver = "org.postgresql.Driver"
  )

  /*def go[T](f: Session => T) = {

  }*/
}

object PostgresSupport extends PostgresSupport

case class Tweet(
  tweetId:      Long,
  created:      DateTime,
  lastModified: DateTime,
  content:      String,
  retweeted:    Boolean,
  username:     String
)

object TweetDAO extends PostgresSupport {
  import CSVConverter._

  object Tweets extends Table[Tweet](/*Some("tweetSchema"),*/"tweets") {
    def tweetId      = column[Long]     ("tweetId", O.AutoInc, O.PrimaryKey, O.DBType("BIGINT"))
    def created      = column[DateTime] ("created", O.DBType("TIMESTAMP"))
    def lastModified = column[DateTime] ("lastmodified", O.DBType("TIMESTAMP"))
    def content      = column[String]   ("content", O.DBType("VARCHAR(140)"))
    def retweeted    = column[Boolean]  ("retweeted", O.DBType("BOOLEAN"))
    def username     = column[String]   ("username", O.DBType("VARCHAR(10)"))

    def *            = (tweetId ~ created ~ lastModified ~ content ~ retweeted ~ username) <> (Tweet, Tweet.unapply _)

    def forInsert    = (created ~ lastModified ~ content ~ retweeted ~ username) returning tweetId
    def tweetIdx     = index("INDEX", tweetId, unique = true)

    def findById     = createFinderBy(_.tweetId)
    def findByCr     = createFinderBy(_.created)
    def findByLast   = createFinderBy(_.lastModified)
    def findByUser   = createFinderBy(_.username)
  }

  // Show createStatements in the REPL

  def listAllTweets = db.withSession {
    pretty(Query(Tweets).sortBy(_.tweetId.asc).list)
  }

  def mostRecent = db.withSession {
    Query(Tweets).sortBy(_.created).list
  }

  def addMultipleTweets(args: List[(String, String)]) = {
    args.map(arg => addTweet(arg._1, arg._2)).map(result => println(result))
  }

  def addTweet(username: String, content: String) = db.withSession {
    val now = new DateTime()
    Tweets.forInsert.insert(now, now, content, false, username) match {
      case 0 => "Something went wrong"
      case n => "Tweet number    " + n + " added successfully"
    }
  }

  def fetchTweetById(tweetId: Long) = db.withSession {
    Query(Tweets).where(_.tweetId is tweetId).first
  }

  /*def unionQueryExample = db.withSession {
    (for {
      q1 <- Query(Tweets).filter(_.content > "b")
      q2 <- Query(Tweets).filter(_.content < "m")
      val union = q1 union q2
      // val unionAll = q1 unionAll q2
    } yield union).list
  }*/

  val tweetByIdRange = for {
    (min, max) <- Parameters[(Long, Long)]
    t <- Tweets if t.tweetId > min && t.tweetId < max
  } yield t

  def deleteTweetById(id: Long) = db.withSession {
    Tweets.filter(_.tweetId === id).delete match {
      case 0 => "0 tweets deleted"
      case 1 => "1 tweet successfully deleted"
      case n => n + " tweets successfully deleted"
    }
  }

  def modifyTweetById(tweetId: Long) = db.withSession {
    val now = new DateTime()
    Tweets.where(_.tweetId is tweetId).
      map(t => t.lastModified).
      update(now) match {
        case 1 => println("Tweet was successfully marked as modified")
        case _ => println("Something went wrong")
      }
  }

  def findByMultipleIds(ids: Long*) = db.withSession {
    Tweets.where(_.tweetId inSetBind ids).map(tweet => tweet).list
  }

  def sortTweetsAlphabetically = db.withSession {
    Query(Tweets).sortBy(_.content.asc).list.map(t => t.content)
  }

  def retweetById(id: Long) = db.withSession {
    Tweets.where(_.tweetId === id).
      map(t => t.retweeted).
      update(true) match {
        case 1 => println("Tweet was successfully marked as retweeted")
        case _ => println("Something went wrong")
      }
  }

  def retweetByIds(ids: Long*) = db.withSession {
    Tweets.where(_.tweetId inSetBind ids).
      where(_.retweeted === false).
      map(t => t.retweeted).update(true) match {
        case 1 => println("All tweets were successfully marked as retweeted")
        case _ => println("Something went wrong")
      }
  }

  def numberOfTweets = db.withSession {
    Query(Tweets).list.length
  }

  def createTables = db.withSession {
    Tweets.ddl.create
  }

  def dropTables = db.withSession {
    Tweets.ddl.drop
  }

  def showSQL = {
    Tweets.ddl.createStatements.foreach(println(_))
  }

  def populateDB(filename: String) = {
    addMultipleTweets(CSVConverter.convert(filename))
  }

  def pretty(tweetList: List[Tweet]) = {
    tweetList.map(t => println(t.tweetId + ": " + 
                               t.username + ", " + 
                               t.content + ", created: " +
                               t.created.toDate.toString))
  }
}

object CSVConverter {
  import java.io.File
  import scala.collection.mutable.ListBuffer

  def convert(filename: String) = {
    val reader = CSVReader.open(new File(filename))
    val rawList = reader.iterator.toList
    val tweets = new ListBuffer[(String, String)]
    rawList.foreach(line => tweets ++= List((line(0), line(1))))
      tweets.toList
  }
}

/*

For the SCALA REPL:

import scala.slick.driver.PostgresDriver.simple._
import Database.threadLocalSession
import com.github.tototoshi.slick.JodaSupport._
import org.joda.time.DateTime
import scala.slick.driver.PostgresDriver
import com.github.tminglei.slickpg._
import com.github.tototoshi.csv._
import TweetDAO._
import TweetDAO.Tweets
import CSVConverter._
val T = TweetDAO

*/