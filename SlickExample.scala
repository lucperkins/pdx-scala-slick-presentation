// import scala.slick.lifted.BaseTypeMapper
// import scala.slick.driver.BasicProfile
import scala.slick.driver.PostgresDriver.simple._
import Database.threadLocalSession
import com.github.tototoshi.slick.JodaSupport._
import org.joda.time.DateTime
import scala.util.{ Try, Success, Failure }
import scala.slick.driver.PostgresDriver
import com.github.tminglei.slickpg._
// import SlickPGSupport._
import com.github.tototoshi.csv._

// import org.joda.time.format.ISODateTimeFormat

object PostgresSupport {
	val db = Database.forURL(
		url = "jdbc:postgresql://localhost:5432/pdx-scala",
		driver = "org.postgresql.Driver"
	)
}

case class Tweet(tweetId:      Long,
	             created:      DateTime,
	             lastModified: DateTime,
	             content:      String,
	             retweeted:    Boolean,
	             username:     String)
	
case class User(userId:   Long,
	            username: String)

object TweetDAO /*extends SlickPGSupport*/ {
	import CSV._
	import PostgresSupport._

	object Tweets extends Table[Tweet](/*Some("tweetSchema"),*/"tweets") {
		def tweetId      = column[Long]     ("tweetId", O.AutoInc, O.PrimaryKey, O.DBType("BIGINT"))
		def created      = column[DateTime] ("created", O.DBType("TIMESTAMP"))
		def lastModified = column[DateTime] ("lastmodified", O.DBType("TIMESTAMP"))
		def content      = column[String]   ("content", O.DBType("VARCHAR(140)"))
		def retweeted    = column[Boolean]  ("retweeted", O.DBType("BOOLEAN"))
		def username     = column[String]   ("username", O.DBType("VARCHAR(10)"))

		def *            = (tweetId ~ created ~ lastModified ~ content ~ retweeted ~ username) <> (Tweet, Tweet.unapply _)
		def forInsert    = (created ~ lastModified ~ content ~ retweeted ~ username) returning tweetId
		def findById     = createFinderBy(_.tweetId)
        def findByCr     = createFinderBy(_.created)
        def findByLast   = createFinderBy(_.lastModified)
	}

	/*object Users extends Table[User]("users") {
		def userId   = column[Long]   ("userId", O.AutoInc, O.PrimaryKey, O.DBType("BIGINT"))
		def username = column[String] ("username", O.DBType("VARCHAR(10)"))
		def *        = (userId ~ username) <> (User, User.unapply _)
		def autoInc  = username returning userId
	}*/

	lazy val allTweets: List[Tweet] = db.withSession {
		Query(Tweets).list
	}

	def addMultipleTweets(args: List[(String, String)]) = {
		Try(args.map(arg => addTweet(arg._1, arg._2))) match {
			case Success(_) => println("Tweets have been successfully added")
			case Failure(_) => println("Something went wrong")
		}
	}

	def addTweet(username: String, content: String) = {
		db.withSession {
			val now = new DateTime()
			Try (Tweets.forInsert.insert(now, now, content, false, username)) match {
				case Success(_) => println("Tweet has been successfully added")
				case Failure(_) => println("Something went wrong")
			}
		}
	}

	def fetchTweetById(tweetId: Long) = {
		db.withSession {
			Tweets.where(_.tweetId is tweetId).list
		}
	}

    def unionQueryExample = db.withSession {
        ((Query(Tweets).filter(_.content > "b")) union (Query(Tweets).filter(_.content < "m"))).list
    }

    val tweetByIdRange = for {
        (min, max) <- Parameters[(Long, Long)]
        t <- Tweets if t.tweetId > min && t.tweetId < max
    } yield t

    /*
    def deleteTweetById(id: Long) = {
		db.withSession {
			Tweets.filter(_.tweetId === id).delete match {
				case 1 => println("Tweet was successfully deleted")
				case _ => println("Something went wrong")
			}
		}
	}
    */

	def modifyTweetById(tweetId: Long) = {
		db.withSession {
			val now = new DateTime()
			Try (Tweets.where(_.tweetId is tweetId).
				        map(t => t.lastModified).
				        update(now)) match {
				case Success(_) => println("Tweet was successfully modified")
				case Failure(_) => println("Something went wrong")
			}
		}
	}

    /* 
    def fetchTweetByUser(id: Long) = {
        db.withSession {
            Tweets.where(_.tweetId === userId).list
        }
    }
    */

	def findByMultipleIds(ids: Long*) = {
		db.withSession {
			Tweets.where(_.tweetId inSetBind ids).map(tweet => tweet).list
		}
	}

	def sortTweetsAlphabetically = {
		db.withSession {
			allTweets.sortBy(_.content).map(t => t.content)
		}
	}

	def tweetContentByLetter(letter: Char) = {
		db.withSession {
			allTweets.filter(_.content.charAt(0) == letter)
		}
	}

	def retweetById(id: Long) = {
		db.withSession {
			Try (Tweets.where(_.tweetId === id).
				        map(t => t.retweeted).
				        update(true)
				) match {
				case Success(_) => println("Tweet was successfully marked as retweeted")
				case Failure(_) => println("Something went wrong")
			}
		}
	}

	def retweetByIds(ids: Long*) = {
		db.withSession {
			Try (Tweets.where(_.tweetId inSetBind ids).
				        where(_.retweeted === false).
				        map(t => t.retweeted).update(true)
			) match {
				case Success(_) => println("All tweets were successfully marked as retweeted")
				case Failure(_) => println("Something went wrong")
			}
		}
	}

	def numberOfTweets = {
		db.withSession {
			allTweets.length
		}
	}

	/*
    def bigJoin(id: Long) = db.withSession {
		(for {
			tweet <- Tweets if tweet.tweetId === id
			user <- Users if user.userId === id
		} yield tweet.content).list
	}
    */

	def createTables = db.withSession {
        val ddl = Tweets.ddl
		Try (ddl.create) match {
			case Success(_) => println("Tables successfully created")
			case Failure(e) => println(e.getMessage)
		}
	}

	def dropTables = {
		db.withSession {
			Try (Tweets.ddl.drop) match {
				case Success(_) => println("Tables successfully dropped")
				case Failure(e) => println(e.getMessage)
			}
		}
	}

	def showSQL     = {
		Tweets.ddl.createStatements.foreach(println(_))
	}

    def populateDB(filename: String) = {
        addMultipleTweets(CSV.convertCSVToTweets(filename))
    }
}

object CSV {
    import java.io.File
    import scala.collection.mutable.ListBuffer

    def convertCSVToTweets(filename: String) = {
        val reader = CSVReader.open(new File(filename))
        val rawList = reader.iterator.toList
        val tweets = new ListBuffer[(String, String)]
        rawList.foreach(line => tweets ++= List((line(0), line(1))))
        tweets.toList
    }
}

/*

For the SCALA REPL:

import scala.slick.lifted._
import scala.slick.driver.PostgresDriver.simple._
import Database.threadLocalSession
import com.github.tototoshi.slick.JodaSupport._
import org.joda.time.DateTime
val db = PostgresSupport.db
import TweetDAO._
import TweetDAO.Tweets
import CSV._

*/