# Generating ANTLR

Assuming that you have a copy of ANTLR's Jar in a data folder at root;

`java -jar ..\..\data\antlr-4.13.2-complete.jar -Dlanguage=Go -visitor -o . .\NSQL.g4`