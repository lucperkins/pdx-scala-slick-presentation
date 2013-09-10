resolvers ++= List(
  "spray" at "http://repo.spray.io",
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++= List(
  "com.typesafe.slick" %% "slick" % "1.0.0",
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  "com.h2database" % "h2" % "1.3.166",
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
  "com.github.tototoshi" %% "slick-joda-mapper" % "0.3.0",
  "com.github.tminglei" % "slick-pg_2.10.1" % "0.1.3",
  "com.github.tototoshi" %% "scala-csv" % "1.0.0-SNAPSHOT"
)