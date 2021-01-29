#!/usr/bin/env bash
#sbt "project database_server" clean publishLocal
sbt "project database_jdbc" "set test in assembly := {}" clean assembly
cp ./app/database-jdbc/target/scala-2.12/qwery-jdbc-assembly-*.jar ~/.dbvis/jdbc/
