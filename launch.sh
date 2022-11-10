#!/bin/bash

./compile.sh
spark-submit --class xke.local.HelloWorld "target/word-count-0.0.1-SNAPSHOT.jar"  