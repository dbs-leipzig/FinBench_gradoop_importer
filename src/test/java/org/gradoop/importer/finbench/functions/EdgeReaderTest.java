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
package org.gradoop.importer.finbench.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EdgeReaderTest extends AbstractTestBase {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ExecutionEnvironment env;
    private EdgeReader edgeReader;

    @Before
    public void setup() {
        env = ExecutionEnvironment.createCollectionsEnvironment();
        edgeReader = new EdgeReader(env);
    }

    @Test
    public void testReadingTransfer() throws Exception {
        // Create a sample CSV file for transfers
        File transferFile = createSampleTransferCSV();

        // Create a DataSet of source and target vertices
        DataSet<TemporalVertex> sourceVertices = createSampleVertices("source-id");
        DataSet<TemporalVertex> targetVertices = createSampleVertices("target-id");

        // Read the CSV file using EdgeReader
        DataSet<TemporalEdge> transferEdges = edgeReader.readingTransfer(transferFile.getAbsolutePath(), sourceVertices, targetVertices);

        // Collect the results
        List<TemporalEdge> results = transferEdges.collect();

        // Assertions
        assertNotNull(results);
        assertEquals(1, results.size());

        TemporalEdge edge = results.get(0);
        assertEquals("Transfer", edge.getLabel());
        assertEquals("source-id", edge.getPropertyValue("SourceID").toString());
        assertEquals("target-id", edge.getPropertyValue("TargetID").toString());
        assertEquals(1000.0, edge.getPropertyValue("Amount").getDouble(), 0);
        assertEquals("2022-08-05 10:15:30", edge.getPropertyValue("CreateTime").toString());
        assertEquals("order123", edge.getPropertyValue("OrderNum").toString());
        assertEquals("payment", edge.getPropertyValue("Comment").toString());
        assertEquals("card", edge.getPropertyValue("PayType").toString());
        assertEquals("electronics", edge.getPropertyValue("GoodsType").toString());

        long expectedValidFrom = convertTimeToUnix("2022-08-05 10:15:30");
        assertEquals(expectedValidFrom, edge.getValidFrom().longValue());
    }

    private File createSampleTransferCSV() throws IOException {
        File file = tempFolder.newFile("transfers.csv");
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("SourceID|TargetID|Amount|CreateTime|OrderNum|Comment|PayType|GoodsType\n");
            writer.write("source-id|target-id|1000.0|2022-08-05 10:15:30|order123|payment|card|electronics\n");
        }
        return file;
    }

    private DataSet<TemporalVertex> createSampleVertices(String id) {
        TemporalVertexFactory vertexFactory = new TemporalVertexFactory();
        Properties properties = Properties.create();
        properties.set("ID", id);
        TemporalVertex vertex = vertexFactory.initVertex(GradoopId.get(),"Label",properties);
        return env.fromElements(vertex);
    }

    private long convertTimeToUnix(String timeString) throws ParseException {
        return org.gradoop.importer.finbench.functions.HelperFunction.convertTimeToUnix(timeString);
    }
}


