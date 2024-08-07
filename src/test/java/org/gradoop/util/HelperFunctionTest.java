package org.gradoop.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.text.ParseException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HelperFunctionTest extends AbstractTestBase {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ExecutionEnvironment env;

    @Before
    public void setup() {
        env = ExecutionEnvironment.createCollectionsEnvironment();
    }

    @Test
    public void testGenerateIdPairs() throws Exception {
        // Create a DataSet of TemporalVertex objects
        DataSet<TemporalVertex> vertices = createSampleVertices();

        // Generate ID pairs using the HelperFunction
        DataSet<Tuple2<String, GradoopId>> idPairs = HelperFunction.generateIdPairs(vertices);

        // Collect the results
        List<Tuple2<String, GradoopId>> results = idPairs.collect();

        // Assertions
        assertNotNull(results);
        assertEquals(2, results.size());

        Tuple2<String, GradoopId> firstPair = results.get(0);
        Tuple2<String, GradoopId> secondPair = results.get(1);

        assertEquals("vertex1", firstPair.f0);
        assertEquals("vertex2", secondPair.f0);

        // Check that GradoopIds are correctly mapped
        assertNotNull(firstPair.f1);
        assertNotNull(secondPair.f1);
    }

    private DataSet<TemporalVertex> createSampleVertices() {
        TemporalVertexFactory vertexFactory = new TemporalVertexFactory();

        Properties properties1 = Properties.create();
        properties1.set("ID", "vertex1");
        TemporalVertex vertex1 = vertexFactory.initVertex( GradoopId.get(), "Label", properties1);

        Properties properties2 = Properties.create();
        properties2.set("ID", "vertex2");
        TemporalVertex vertex2 = vertexFactory.initVertex(GradoopId.get(), "Label", properties2);

        return env.fromElements(vertex1, vertex2);
    }

    @Test
    public void testConvertTimeToUnixWithMillis() throws ParseException {
        String timestamp = "2024-08-05 01:23:45.678";
        String timestamp2 = "1998-04-06 17:30:00.000";
        long unixTime = 1722813825678L;
        long unixTime2 = 891876600000L;
        assertEquals(unixTime, HelperFunction.convertTimeToUnix(timestamp));
        assertEquals(unixTime2, HelperFunction.convertTimeToUnix(timestamp2));
    }
}
