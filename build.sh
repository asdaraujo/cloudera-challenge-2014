#!/bin/bash

export JAVA_HOME=${JAVA_HOME:-$( readlink -f /usr/java/default )}
export PATH=$PATH:$JAVA_HOME/bin
mvn clean package
