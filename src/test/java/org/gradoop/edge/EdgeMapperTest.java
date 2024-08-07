package org.gradoop.edge;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EdgeMapperTest extends AbstractTestBase {

    private EdgeMapper edgeMapper;
    private TemporalEdgeFactory edgeFactory;

    @Before
    public void setup() {
        edgeFactory = new TemporalEdgeFactory();
        edgeMapper = new EdgeMapper(edgeFactory);
    }

    @Test
    public void testMapTransfer() throws ParseException {
        // Define the input tuple
        Tuple8<String, String, Double, String, String, String, String, String> edgeData =
                new Tuple8<>("source-id", "target-id", 1000.0, "2022-08-05 10:15:30", "order123", "payment", "card", "electronics");
        GradoopId sourceId = GradoopId.get();
        GradoopId targetId = GradoopId.get();

        Tuple2<Tuple8<String, String, Double, String, String, String, String, String>, GradoopId> intermediateTuple =
                new Tuple2<>(edgeData, sourceId);
        Tuple2<Tuple2<Tuple8<String, String, Double, String, String, String, String, String>, GradoopId>, GradoopId> inputTuple =
                new Tuple2<>(intermediateTuple, targetId);

        // Map the input tuple to a TemporalEdge
        TemporalEdge result = edgeMapper.mapTransfer(inputTuple);

        // Assertions
        assertNotNull(result);
        assertEquals("Transfer", result.getLabel());
        assertEquals(sourceId, result.getSourceId());
        assertEquals(targetId, result.getTargetId());

        Properties properties = result.getProperties();
        assertEquals("source-id", properties.get("SourceID").toString());
        assertEquals("target-id", properties.get("TargetID").toString());
        assertEquals(1000.0, properties.get("Amount").getDouble(), 0);
        assertEquals("2022-08-05 10:15:30", properties.get("CreateTime").toString());
        assertEquals("order123", properties.get("OrderNum").toString());
        assertEquals("payment", properties.get("Comment").toString());
        assertEquals("card", properties.get("PayType").toString());
        assertEquals("electronics", properties.get("GoodsType").toString());

        long expectedValidFrom = convertTimeToUnix("2022-08-05 10:15:30");
        assertEquals(expectedValidFrom, result.getValidFrom().longValue());
    }

    private long convertTimeToUnix(String timeString) throws ParseException {
        return org.gradoop.util.HelperFunction.convertTimeToUnix(timeString);
    }
}
