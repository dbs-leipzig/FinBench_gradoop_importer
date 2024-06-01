package org.gradoop.edge;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.io.Serializable;

import static org.gradoop.util.HelperFunction.*;

/**
 * The EdgeReader class is responsible for reading CSV files containing edge data and mapping them
 * to TemporalEdge objects using suitable mapper.
 */

public class EdgeReader implements Serializable {

    transient ExecutionEnvironment env;
    TemporalEdgeFactory factory;
    EdgeMapper edgeMapper;

    public EdgeReader(ExecutionEnvironment env) {
        this.env = env;
        this.factory = new TemporalEdgeFactory();
        this.edgeMapper = new EdgeMapper(this.factory);
    }

    /**
     * Reads a CSV file containing transfer data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file
     * @param sourceVertices the DataSet of source vertices
     * @param targetVertices the DataSet of target vertices
     * @return a DataSet of TemporalEdge objects representing transfers
     */

    public DataSet<TemporalEdge> readingTransfer(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple8<String, String, Double, String, String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, Double.class, String.class, String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        return csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple8<String, String, Double, String, String, String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple8<String, String, Double, String, String, String, String, String>, GradoopId>, GradoopId>>() {})
                .map(data -> edgeMapper.mapTransfer(data))
                .returns(new TypeHint<TemporalEdge>() {});
    }

    /**
     * Reads a CSV file containing investment data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file.
     * @param sourceVertices the DataSet of source vertices.
     * @param targetVertices the DataSet of target vertices.
     * @return a DataSet of TemporalEdge objects representing investments.
     */

    public DataSet<TemporalEdge> readingInvest(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple,  targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId>>() {})
                .map(data -> edgeMapper.mapInvest(data))
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    /**
     * Reads a CSV file containing ownerships data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file.
     * @param sourceVertices the DataSet of source vertices.
     * @param targetVertices the DataSet of target vertices.
     * @return a DataSet of TemporalEdge objects representing ownerships.
     */

    public DataSet<TemporalEdge> readingOwn(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple3<String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple3<String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple3<String, String, String>, GradoopId>, GradoopId>>() {})
                .map(data -> edgeMapper.mapOwn(data))
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    /**
     * Reads a CSV file containing deposit data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file.
     * @param sourceVertices the DataSet of source vertices.
     * @param targetVertices the DataSet of target vertices.
     * @return a DataSet of TemporalEdge objects representing deposits.
     */

    public DataSet<TemporalEdge> readingDeposit(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices){
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId>>() {})
                .map(data -> edgeMapper.mapDeposit(data))
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    /**
     * Reads a CSV file containing repayment data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file.
     * @param sourceVertices the DataSet of source vertices.
     * @param targetVertices the DataSet of target vertices.
     * @return a DataSet of TemporalEdge objects representing repayments.
     */

    public DataSet<TemporalEdge> readingRepay(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId>>() {})
                .map(data -> edgeMapper.mapRepay(data))
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    /**
     * Reads a CSV file containing guarantees data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file.
     * @param sourceVertices the DataSet of source vertices.
     * @param targetVertices the DataSet of target vertices.
     * @return a DataSet of TemporalEdge objects representing guarantees.
     */


    public DataSet<TemporalEdge> readingGuarantee(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId>>() {})
                .map(data -> edgeMapper.mapGuarantee(data))
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    /**
     * Reads a CSV file containing sign ins data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file.
     * @param sourceVertices the DataSet of source vertices.
     * @param targetVertices the DataSet of target vertices.
     * @return a DataSet of TemporalEdge objects representing sign ins.
     */

    public DataSet<TemporalEdge> readingSignIn(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices){

        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId>>() {})
                .map(data -> edgeMapper.mapSignIn(data))
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    /**
     * Reads a CSV file containing withdrawals data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file.
     * @param sourceVertices the DataSet of source vertices.
     * @param targetVertices the DataSet of target vertices.
     * @return a DataSet of TemporalEdge objects representing withdrawals.
     */

    public DataSet<TemporalEdge> readingWithdraw(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId>>() {})
                .map (data -> edgeMapper.mapWithdraw(data))
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    /**
     * Reads a CSV file containing applications data, extracts needed IDs and maps it to a DataSet of TemporalEdge objects.
     *
     * @param filePath the path to the CSV file.
     * @param sourceVertices the DataSet of source vertices.
     * @param targetVertices the DataSet of target vertices.
     * @return a DataSet of TemporalEdge objects representing applications.
     */


    public DataSet<TemporalEdge> readingApply(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {

        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> sourceIdPairs = generateIdPairs(sourceVertices);
        DataSet<Tuple2<String, GradoopId>> targetIdPairs = generateIdPairs(targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(sourceIdPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(targetIdPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> new Tuple2<>(joinedTuple, targetIds.f1))
                .returns(new TypeHint<Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId>>() {})
                .map(data -> edgeMapper.mapApply(data))
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

}



