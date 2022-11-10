#!/bin/bash

mvn archetype:generate -DarchetypeGroupId=xke.local -DarchetypeArtifactId=word-count


mvn install:install-file -Dfile=non-maven-proj.jar -DgroupId=some.group -DartifactId=non-maven-proj -Dversion=1 -Dpackaging=jar