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
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class VertexReaderTest extends AbstractTestBase {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private ExecutionEnvironment env;
    private VertexReader vertexReader;

    @Before
    public void setup() {
        env = ExecutionEnvironment.createCollectionsEnvironment();
        vertexReader = new VertexReader(env);
    }

    @Test
    public void testReadingPerson() throws Exception {
        // Create a sample CSV file for persons
        File personFile = createSamplePersonCSV();

        // Read the CSV file using VertexReader
        DataSet<TemporalVertex> personVertices = vertexReader.readingPerson(personFile.getAbsolutePath());

        // Collect the results
        List<TemporalVertex> results = personVertices.collect();

        // Assertions
        assertNotNull(results);
        assertEquals(1, results.size());

        TemporalVertex person = results.get(0);
        assertEquals("Person", person.getLabel());
        assertEquals(123L, person.getPropertyValue("id").getLong());
        assertEquals("John Doe", person.getPropertyValue("name").toString());
        assertEquals("false", person.getPropertyValue("isBlocked").toString());
        assertEquals("2022-08-05 10:15:30", person.getPropertyValue("createTime").toString());
        assertEquals("Male", person.getPropertyValue("gender").toString());
        assertEquals("1990-01-01", person.getPropertyValue("birthday").toString());
        assertEquals("USA", person.getPropertyValue("country").toString());
        assertEquals("New York", person.getPropertyValue("city").toString());
    }

    private File createSamplePersonCSV() throws IOException {
        File file = tempFolder.newFile("persons.csv");
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("ID|Name|Is Blocked|CreateTime|Gender|Birthday|Country|City\n");
            writer.write("123|John Doe|false|2022-08-05 10:15:30|Male|1990-01-01|USA|New York\n");
        }
        return file;
    }
}
