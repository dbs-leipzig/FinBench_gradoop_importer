package org.gradoop.query;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.temporal.model.impl.TemporalGraph;



public class SimpleQuery {

    public static DataSet<Tuple3<String, Long, Double>> tsr4(TemporalGraph graph, String id, long startTime, long endTime, double threshold) throws Exception {


        return graph
                .fromTo(startTime, endTime)
                .subgraph(
                        v -> v.getLabel().equals("Account"),
                        e -> e.getLabel().equals("Transfer") && e.getPropertyValue("Amount").getDouble() > threshold
                )
                .query(String.format("MATCH (src:Account {ID: \"%s\"})-[edge:Transfer]->(dst:Account)", id))
                .getEdges()
                .map(edge -> new Tuple3<>(edge.getProperties().get("TargetID").toString(), 1L, edge.getPropertyValue("Amount").getDouble()))
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.DOUBLE))
                .groupBy(0)
                .reduce((a, b) -> new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2));
    }

    public static DataSet<String> tsr5(TemporalGraph graph, String id, long startTime, long endTime, double threshold) throws Exception {
        return graph
                .fromTo(startTime, endTime)
                .subgraph(
                        v -> v.getLabel().equals("Account"),
                        e -> e.getLabel().equals("Transfer") && e.getPropertyValue("Amount").getDouble() > threshold
                )
                .query(String.format(
                        "MATCH (src:Account {ID: \"%s\"})<-[e1:Transfer]-(mid:Account)-[e2:Transfer]->(dst:Account {IsBlocked: true})\n " +
                                "WHERE src.ID <> dst.ID", id))
                .getEdges()
                .map(edge -> edge.getProperties().get("TargetID").toString())
                .distinct();
    }

}
