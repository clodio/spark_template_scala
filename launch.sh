#!/bin/bash

./compile.sh
spark-submit --master local  --class

./spark-submit --class scala.xke.local.HelloWorld MyFatJarFile.jar
