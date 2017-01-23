/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import Versions._
import sbt._

name := "asterixdb-spark-connector"

scalaVersion := scala
organization := "org.apache.asterix"
version := sparkVersion

//Add local repo to resolvers
resolvers += Resolver.mavenLocal

// Add Spark jars on sbt run
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

//Hyracks
libraryDependencies += "org.apache.hyracks" % "hyracks-api" % hyracksVersion exclude("org.slf4j", "slf4j-jcl") exclude("com.fasterxml.jackson.core", "jackson-databind")

libraryDependencies += "org.apache.hyracks" % "hyracks-client" % hyracksVersion exclude("org.slf4j", "slf4j-jcl") exclude("com.fasterxml.jackson.core", "jackson-databind")

libraryDependencies += "org.apache.hyracks" % "hyracks-control-nc" % hyracksVersion exclude("org.slf4j", "slf4j-jcl") exclude("com.fasterxml.jackson.core", "jackson-databind")

libraryDependencies += "org.apache.hyracks" % "hyracks-dataflow-common" % hyracksVersion exclude("org.slf4j", "slf4j-jcl") exclude("com.fasterxml.jackson.core", "jackson-databind")

//Spark
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % sparkScope

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % sparkScope

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % sparkScope

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % sparkScope

libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % sparkScope

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % sparkScope


//Other
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % httpComponentsVersion excludeAll ExclusionRule(organization = "org.slf4j")

libraryDependencies += "net.liftweb" %% "lift-json" % liftJsonVersion

libraryDependencies += "junit" % "junit" % junitVersion % "test"

libraryDependencies += "org.json" % "json" % orgJsonVersion % "provided"

excludeFilter in unmanagedResources := HiddenFileFilter || "*properties" || "*xml"
excludeFilter in unmanagedBase := HiddenFileFilter || "*properties" || "*xml"


val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".RSA" => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case meta(_) => MergeStrategy.first
  case "rootdoc.txt" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
