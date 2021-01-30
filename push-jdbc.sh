#!/usr/bin/env bash
sbt "project database_server" publishLocal
sbt "project database_server_testkit" publishLocal
sbt "project database_jdbc" assembly
cp ./app/database-jdbc/target/scala-2.12/qwery-jdbc-assembly-*.jar ~/.dbvis/jdbc/
