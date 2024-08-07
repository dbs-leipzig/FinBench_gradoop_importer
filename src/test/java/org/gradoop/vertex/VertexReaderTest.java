package org.gradoop.vertex;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
        assertEquals("123", person.getPropertyValue("ID").toString());
        assertEquals("John Doe", person.getPropertyValue("Name").toString());
        assertEquals("false", person.getPropertyValue("Is Blocked").toString());
        assertEquals("2022-08-05 10:15:30", person.getPropertyValue("CreateTime").toString());
        assertEquals("Male", person.getPropertyValue("Gender").toString());
        assertEquals("1990-01-01", person.getPropertyValue("Birthday").toString());
        assertEquals("USA", person.getPropertyValue("Country").toString());
        assertEquals("New York", person.getPropertyValue("City").toString());
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
