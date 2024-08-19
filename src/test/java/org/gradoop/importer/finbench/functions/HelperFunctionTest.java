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
    private GradoopId gradoopId;

    @Before
    public void setup() {
        env = ExecutionEnvironment.createCollectionsEnvironment();
        gradoopId = GradoopId.get();
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
        assertEquals(1, results.size());
        Tuple2<String, GradoopId> idPair = results.get(0);
        assertEquals("123", idPair.f0);
        // Check that GradoopIds are correctly mapped
        assertEquals(idPair.f1, gradoopId);
    }

    private DataSet<TemporalVertex> createSampleVertices() {
        TemporalVertexFactory vertexFactory = new TemporalVertexFactory();

        Properties properties1 = Properties.create();
        properties1.set("id", 123L);
        TemporalVertex vertex = vertexFactory.initVertex(gradoopId, "label", properties1);

        return env.fromElements(vertex);
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
