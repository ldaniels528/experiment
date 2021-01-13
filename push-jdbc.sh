#!/usr/bin/env bash
sbt "project jdbc" clean assembly
cp ~/GitHub/qwery/app/jdbc/target/scala-2.12/qwery-jdbc-assembly-*.jar ~/.dbvis/jdbc/
