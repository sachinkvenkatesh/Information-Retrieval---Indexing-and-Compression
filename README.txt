 ***********This file has instructions on how to execute the project************

Contents:
=========
The project folder contains the following files:
	- DictEntry.java
	- Document.java
	- Indexing.java
	- Porter.java
	- PostingEntry.java
	- Timer.java

Environment:
============
Operating System : Ubuntu
Compiler	 : JRE 1.8.0_45

Instructions to run:
====================
1. source /usr/local/corenlp341/classpath.sh
2. javac -cp .:$CLASSPATH Indexing.java DictEntry.java PostingEntry.java Document.java Timer.java Porter.java
3. java Indexing /people/cs/s/sanda/cs6322/Cranfield/ ~/IR_Assignment2/ /people/cs/s/sanda/cs6322/resourcesIR/stopwords 8

First argument is the path to the folder containing all the files to be indexed, Second argument is the path to store the output files,
third argument is the path for the stop words file and fourth argument is the block size k.

Refernce:
=========
Porter Stemmer is taken from: http://ir.dcs.gla.ac.uk/resources/linguistic_utils/porter.java
Lemmatizer is taken from Stanford NLP.
