[![Apache License, Version 2.0, January 2004](https://img.shields.io/github/license/apache/maven.svg?label=License)](https://www.apache.org/licenses/LICENSE-2.0)
# Gradoop: FinBench Dataset Importer

This is an supporting application for [Gradoop](https://github.com/dbs-leipzig/gradoop), allows importing FinBench datasets as TPGM in Gradoop for further graph analyzing and processing.
This project is built using Apache Flink `1.9.3` and Gradoop `0.7.0-SNAPSHOT`

> This project was developed during a Bachelor's thesis at Leipzig University.

# Requirements:
- Maven.
- Java 8.
- [FinBench Datasets](https://drive.google.com/drive/folders/1NIAo4KptskBytbXoOqmF3Sto4hTX3JIH) provided by LDBC.

## Building
Bulding should be done using Maven. Simply run this in the project directory:

```
mvn package
```

This will generate the JAR package `FinBenchGradoopImporter-1.0.jar` inside `target` folder.

## Execution

This application can be executed using Apache Flink. An example Command is:
```
/bin/flink run -p 128 -c org.gradoop.importer.finbench.FinBenchImporter FinBenchGradoopImporter-1.0.jar -i 
hdfs:///finbench/sf10 -o hdfs:///finbench/gradoop-parquet-protobuf -f protobuf
```


### Configuration

|Parameter| Argument                                     | Description                                                         | Required |
|:-------:|----------------------------------------------|---------------------------------------------------------------------|----------|
|  `-i`   | `/path/to/finbench`                          | The input path to a directory containing all Finbench's csv files.  | Yes      |
|  `-o`   | `/path/out`                                  | The output path for the Gradoop graph to be written.                | Yes      |
|  `-f`   | `csv` or `indexed` or `parquet` or `protobuf` | The output format. CSV is default, parquet or protobuf are fastest. | Yes      |

### Disclaimer

Apache&reg;, Apache Accumulo&reg;, Apache Flink, Flink&reg; are either registered trademarks or trademarks of the Apache Software Foundation
in the United States and/or other countries.