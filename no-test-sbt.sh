#!/bin/bash
sbt "set test in assembly := {}"  "set skip in update := true" "set offline := true" "set scalacOptions in ThisBuild ++= Seq(\"-unchecked\", \"-deprecation\")" "$*"

