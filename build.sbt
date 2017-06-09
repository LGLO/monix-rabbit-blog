name := "monix-amqp"

version := "0.1.0"

organization := "io.scalac"

startYear := Some(2017)

licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

scalaVersion := "2.12.2"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings", "-target:jvm-1.8")

libraryDependencies ++= Seq(
  "org.reactivestreams"      %  "reactive-streams"         % "1.0.0",
  "com.rabbitmq"             %  "amqp-client"              % "4.1.0",
  "io.monix"                 %% "monix"                    % "2.3.0",
  "org.scalatest"            %% "scalatest"                % "3.0.3"   % "test", // for TCK
  "org.reactivestreams"      %  "reactive-streams-tck"     % "1.0.0"   % "test"
)
