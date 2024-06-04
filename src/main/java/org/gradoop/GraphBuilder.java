package org.gradoop;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.datasource.DataSourceExtractor;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.indexed.TemporalIndexedCSVDataSink;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

public class GraphBuilder {

    /**
     * Main program to read in FinBench graph. Arguments included input, output and data sink type
     * Default data sink is CSV unless set differently
     * {@code /path/to/flink run -c org.gradoop.org.gradoop.GraphBuilder path/to/finbenchh-importer.jar -i hdfs:///finbench-dataset -o hdfs:///output -indexed}
     * @param args program arguments
     * @throws Exception in case of error
     */

    private static final String OPTION_INPUT_PATH = "i";
    private static final String OPTION_OUTPUT_PATH = "o";
    private static final String OPTION_INDEXED = "f1";
    private static final String OPTION_PARQUET = "f2";
    private static final String OPTION_PROTOBUF = "f3";
    static String INPUT_PATH;
    static String OUTPUT_PATH;



    public static void main (String[] args) throws Exception {

        //reading in arguments
        Options cliOption = new Options();
        cliOption.addOption(new Option(OPTION_INPUT_PATH, "inputPath", true, "Input path. (REQUIRED)"));
        cliOption.addOption(new Option(OPTION_OUTPUT_PATH, "outputPath", true, "Output path. (REQUIRED)"));
        cliOption.addOption(new Option(OPTION_INDEXED, "dataSinkIndexed", false, "Indexed CSV datasink"));
        cliOption.addOption(new Option(OPTION_PARQUET, "dataSinkParquet", false, "Parquet datasink"));
        cliOption.addOption(new Option(OPTION_PROTOBUF, "dataSinkProtobuff", false, "Protobuff CSV datasink"));

        CommandLine parsedOptions = new DefaultParser().parse(cliOption, args);

        if (!parsedOptions.hasOption('i')|| !parsedOptions.hasOption('o')) {
            System.err.println("No input- or output-path given.");
            System.err.println("See --help for more infos.");
            return;
        }

        INPUT_PATH  = parsedOptions.getOptionValue('i');
        OUTPUT_PATH = parsedOptions.getOptionValue('o');

        //initiating config
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        TemporalGradoopConfig tConfig = TemporalGradoopConfig.fromGradoopFlinkConfig(config);

        //reading files, datasource and creating graph
        DataSourceExtractor dS = new DataSourceExtractor(env);
        Tuple2<DataSet<TemporalVertex>, DataSet<TemporalEdge>> dataSource = dS.readingFinbench(INPUT_PATH);
        TemporalGraph tg = tConfig.getTemporalGraphFactory().fromDataSets(dataSource.f0, dataSource.f1);

        //exporting into selected datasink
        if (parsedOptions.hasOption(OPTION_INDEXED)){
            tg.writeTo(new TemporalIndexedCSVDataSink(OUTPUT_PATH, config), true);
            env.execute("Import Finbench Indexed CSV in Gradoop: " + INPUT_PATH.substring(INPUT_PATH.lastIndexOf('/') + 1) + ". P:" + env.getParallelism());
        } else if (parsedOptions.hasOption(OPTION_PARQUET)) {
            tg.writeTo(new TemporalIndexedCSVDataSink(OUTPUT_PATH, config), true);
            env.execute("Import Finbench Parquet in Gradoop: " + INPUT_PATH.substring(INPUT_PATH.lastIndexOf('/') + 1) + ". P:" + env.getParallelism());
        } else if (parsedOptions.hasOption(OPTION_PROTOBUF)) {
            tg.writeTo(new TemporalIndexedCSVDataSink(OUTPUT_PATH, config), true);
            env.execute("Import Finbench Protobuf CSV in Gradoop: " + INPUT_PATH.substring(INPUT_PATH.lastIndexOf('/') + 1) + ". P:" + env.getParallelism());
        } else {
            tg.writeTo(new TemporalCSVDataSink(OUTPUT_PATH, config), true);
            env.execute("Import Finbench CSV in Gradoop: " + INPUT_PATH.substring(INPUT_PATH.lastIndexOf('/') + 1) + ". P:" + env.getParallelism());
        }
    }
}