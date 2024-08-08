/*
 * Copyright © 2014 - 2024 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.importer.finbench;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.indexed.TemporalIndexedCSVDataSink;
import org.gradoop.temporal.io.impl.parquet.plain.TemporalParquetDataSink;
import org.gradoop.temporal.io.impl.parquet.protobuf.TemporalParquetProtobufDataSink;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopConfig;

public class FinBenchImporter {

    /**
     * Main program to read in FinBench graph. Arguments included input, output and data sink type
     * Default data sink is CSV unless set differently
     * {@code /path/to/flink run -c org.gradoop.org.gradoop.importer.finbench.GraphBuilder path/to/finbenchh-importer.jar -i hdfs:///finbench-dataset -o hdfs:///output -indexed}
     * @param args program arguments
     * @throws Exception in case of error
     */

    private static final String OPTION_INPUT_PATH = "i";
    private static final String OPTION_OUTPUT_PATH = "o";
    private static final String OPTION_FORMAT = "f";
    static String INPUT_PATH;
    static String OUTPUT_PATH;
    static String OUTPUT_FORMAT;

    public static void main (String[] args) throws Exception {

        //reading in arguments
        Options cliOption = new Options();
        cliOption.addOption(new Option(OPTION_INPUT_PATH, "inputPath", true, "Input path. (REQUIRED)"));
        cliOption.addOption(new Option(OPTION_OUTPUT_PATH, "outputPath", true, "Output path. (REQUIRED)"));
        cliOption.addOption(new Option(OPTION_FORMAT, "outputFormat", true, "Output Gradoop format."));

        CommandLine parsedOptions = new DefaultParser().parse(cliOption, args);

        if (!parsedOptions.hasOption('i')|| !parsedOptions.hasOption('o')) {
            System.err.println("No input- or output-path given.");
            System.err.println("See --help for more infos.");
            return;
        }

        INPUT_PATH  = parsedOptions.getOptionValue(OPTION_INPUT_PATH);
        OUTPUT_PATH = parsedOptions.getOptionValue(OPTION_OUTPUT_PATH);
        OUTPUT_FORMAT = parsedOptions.getOptionValue(OPTION_FORMAT);

        //initiating config
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TemporalGradoopConfig config = TemporalGradoopConfig.createConfig(env);

        //reading files, datasource and creating graph
        FinBenchTemporalDataSource extractor = new FinBenchTemporalDataSource(INPUT_PATH, env, config);

        TemporalGraph tg = extractor.getTemporalGraph();

        TemporalDataSink sink;

        //exporting into selected datasink
        switch (OUTPUT_FORMAT) {
        case "csv":
            sink = new TemporalCSVDataSink(OUTPUT_PATH, config);
            break;
        case "indexed":
            sink = new TemporalIndexedCSVDataSink(OUTPUT_PATH, config);
            break;
        case "parquet":
            sink = new TemporalParquetDataSink(OUTPUT_PATH, config);
            break;
        case "protobuf":
            sink = new TemporalParquetProtobufDataSink(OUTPUT_PATH, config);
            break;
        default:
            throw new IllegalArgumentException("Unknown output format: " + OUTPUT_FORMAT);
        }

        tg.writeTo(sink, true);

        env.execute("Create Gradoop FinBench dataset in format " + OUTPUT_FORMAT + "with parallelism of " + env.getParallelism() + ".");
    }
}