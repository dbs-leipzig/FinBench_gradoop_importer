# Gradoop: FinBench Dataset Importer

This is an supporting application for [Gradoop](https://github.com/dbs-leipzig/gradoop), allows importing FinBench datasets as TPGM in Gradoop for further graph analyzing and processing.
This project is built using Apache Flink `1.9.3` and Gradoop `0.7.0-SNAPSHOT`

# Requirements:
- Maven.
- Java 8.
- [FinBench Datasets](https://drive.google.com/drive/folders/1NIAo4KptskBytbXoOqmF3Sto4hTX3JIH) provided by LDBC.

## Building
Bulding should be done using Maven. Simply run this in the project directory:

```
mvn package
```

This will generate the JAR package `FinBenchImporter-1.0-minimal.jar` inside target folder.

## Execution

This application can be executed using a Flink client. Run
```
flink run -c org.gradoop.FinbenchImporter <path/to/jar/package> org.gradoop.FinbenchImporter
```


### Configuration

|Parameter| Argument | Description | Required |
|:-------:|----------|-------------|-------------|
| `-i`    | INPUT    | The input path to a directory containing all Finbench's csv files  | Yes|
| `-o`    | OUTPUT   | The output path for graph to be written out | Yes|
| `-f1`    | (none)   | Graph output in IndexedCSV format |No|
| `-f2`    | (none)   | Graph output in Parquet format |No|
| `-f3`    | (none)   | Graph output in Protobuf format |No|

You should pick at most one from `-f1`, `-f2`, `-f3` for the ouput graph data format. If none is selected, the default export format is CSV.

