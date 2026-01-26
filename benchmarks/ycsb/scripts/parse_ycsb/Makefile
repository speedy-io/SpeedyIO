# Makefile to build MergeHdrStats.java
# YCSB_DIR := /users/gargsaab/ssd/ycsb

JSON_JAR := ./jar/json-20230227.jar
HDR_JAR := ./jar/HdrHistogram-2.2.2.jar

CLASSPATH := .:$(HDR_JAR):$(JSON_JAR)

all: MergeHdrStats.class

MergeHdrStats.class: MergeHdrStats.java
	javac -cp "$(CLASSPATH)" MergeHdrStats.java

clean:
	rm -f *.class

# Use command like this to run: java -cp .:./jar/HdrHistogram-2.2.2.jar:./jar/json-20230227.jar MergeHdrStats stats.json READ.hdr READ2.hdr READ3.hdr ....
