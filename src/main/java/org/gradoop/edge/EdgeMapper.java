package org.gradoop.edge;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;

import java.io.Serializable;
import java.text.ParseException;

import static org.gradoop.util.HelperFunction.convertTimeToUnix;

/**
 * The EdgeMapper class is responsible for mapping tuples to TemporalEdge objects for various types of edges.
 */

public class EdgeMapper implements Serializable {

    private TemporalEdgeFactory factory;

    public EdgeMapper(TemporalEdgeFactory factory){
        this.factory = factory;
    }

    /**
     * Maps a tuple representing a transfer to a TemporalEdge.
     *
     * @param data the tuple containing transfer data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */


    public TemporalEdge mapTransfer(Tuple2<Tuple2<Tuple8<String, String, Double, String, String, String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple8<String, String, Double, String, String, String, String, String> edgeData = data.f0.f0;

        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;
        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("Amount", edgeData.f2);
        edgeProps.set("CreateTime", edgeData.f3);
        edgeProps.set("OrderNum", edgeData.f4);
        edgeProps.set("Comment", edgeData.f5);
        edgeProps.set("PayType", edgeData.f6);
        edgeProps.set("GoodsType", edgeData.f7);

        TemporalEdge edge = factory.createEdge("Transfer", sourceId, targetId, edgeProps);

        edge.setValidFrom(convertTimeToUnix(edgeData.f3));

        return edge;
    }

    /**
     * Maps a tuple representing an investment to a TemporalEdge.
     *
     * @param data the tuple containing investment data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */

    public TemporalEdge mapInvest(Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple4<String, String, String, String> edgeData = data.f0.f0;

        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;
        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("InvestmentAmount", edgeData.f2);
        edgeProps.set("CreateTime", edgeData.f3);


        TemporalEdge edge = factory.createEdge("Invest", sourceId, targetId, edgeProps);
        edge.setValidFrom(convertTimeToUnix(edgeData.f3));

        return edge;
    }

    /**
     * Maps a tuple representing an ownership to a TemporalEdge.
     *
     * @param data the tuple containing ownership data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */

    public TemporalEdge mapOwn(Tuple2<Tuple2<Tuple3<String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple3<String, String, String> edgeData = data.f0.f0;

        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;
        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("CreateTime", edgeData.f2);

        TemporalEdge edge = factory.createEdge("Own", sourceId, targetId, edgeProps);
        edge.setValidFrom(convertTimeToUnix(edgeData.f2));

        return edge;
    }

    /**
     * Maps a tuple representing a deposit to a TemporalEdge.
     *
     * @param data the tuple containing deposit data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */

    public TemporalEdge mapDeposit(Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple4<String, String, String, String> edgeData = data.f0.f0;
        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;

        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("Amount", edgeData.f2);
        edgeProps.set("CreateTime", edgeData.f3);

        TemporalEdge edge = factory.createEdge("Deposit", sourceId, targetId, edgeProps);

        edge.setValidFrom(convertTimeToUnix(edgeData.f3));

        return edge;
    }

    /**
     * Maps a tuple representing a repayment to a TemporalEdge.
     *
     * @param data the tuple containing repayment data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */

    public TemporalEdge mapRepay(Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple4<String, String, String, String> edgeData = data.f0.f0;
        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;

        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("Amount", edgeData.f2);
        edgeProps.set("CreateTime", edgeData.f3);

        TemporalEdge edge = factory.createEdge("Repay", sourceId, targetId, edgeProps);

        edge.setValidFrom(convertTimeToUnix(edgeData.f3));

        return edge;
    }

    /**
     * Maps a tuple representing a sign in to a TemporalEdge.
     *
     * @param data the tuple containing sign in data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */

    public TemporalEdge mapSignIn(Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple4<String, String, String, String> edgeData = data.f0.f0;
        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;

        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("CreateTime", edgeData.f2);
        edgeProps.set("Location", edgeData.f3);

        TemporalEdge edge = factory.createEdge("SignIn", sourceId, targetId, edgeProps);

        edge.setValidFrom(convertTimeToUnix(edgeData.f2));

        return edge;
    }

    /**
     * Maps a tuple representing a withdrawal to a TemporalEdge.
     *
     * @param data the tuple containing withdrawal data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */

    public TemporalEdge mapWithdraw(Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple4<String, String, String, String> edgeData = data.f0.f0;
        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;

        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("Amount", edgeData.f2);
        edgeProps.set("CreateTime", edgeData.f3);

        TemporalEdge edge = factory.createEdge("Withdraw", sourceId, targetId, edgeProps);

        edge.setValidFrom(convertTimeToUnix(edgeData.f3));

        return edge;
    }

    /**
     * Maps a tuple representing a guarantee to a TemporalEdge.
     *
     * @param data the tuple containing guarantee data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */

    public TemporalEdge mapGuarantee(Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple4<String, String, String, String> edgeData = data.f0.f0;
        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;

        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("CreateTime", edgeData.f2);
        edgeProps.set("Relation", edgeData.f3);

        TemporalEdge edge = factory.createEdge("Guarantee", sourceId, targetId, edgeProps);

        edge.setValidFrom(convertTimeToUnix(edgeData.f2));

        return edge;
    }

    /**
     * Maps a tuple representing an application to a TemporalEdge.
     *
     * @param data the tuple containing application data
     * @return the created TemporalEdge for the transfer
     * @throws ParseException if an error occurs during converting DateTime to UNIX
     */

    public TemporalEdge mapApply(Tuple2<Tuple2<Tuple4<String, String, String, String>, GradoopId>, GradoopId> data) throws ParseException {

        Tuple4<String, String, String, String> edgeData = data.f0.f0;
        GradoopId sourceId = data.f0.f1;
        GradoopId targetId = data.f1;

        Properties edgeProps = Properties.create();

        edgeProps.set("SourceID", edgeData.f0);
        edgeProps.set("TargetID", edgeData.f1);
        edgeProps.set("CreateTime", edgeData.f2);
        edgeProps.set("Org", edgeData.f3);

        TemporalEdge edge = factory.createEdge("Apply", sourceId, targetId, edgeProps);

        edge.setValidFrom(convertTimeToUnix(edgeData.f2));

        return edge;
    }
}
