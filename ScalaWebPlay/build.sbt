name := "ScalaWebPlay"
version := "1.0"
scalaVersion := "2.12.3"
      
lazy val `scalawebplay` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
resolvers += "Akka Snapshot Repository" at "https://repo.akka.io/snapshots/"

libraryDependencies ++= Seq( jdbc , ehcache , ws , specs2 % Test , guice )
libraryDependencies ++= Seq(
  "org.reactivemongo" %% "play2-reactivemongo" % "0.20.11-play27"
)

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )