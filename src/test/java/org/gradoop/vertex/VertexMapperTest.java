package org.gradoop.vertex;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class VertexMapperTest extends AbstractTestBase {

    private VertexMapper.PersonMapper personMapper;
    private TemporalVertexFactory vertexFactory;

    @Before
    public void setup() {
        vertexFactory = new TemporalVertexFactory();
        personMapper = new VertexMapper.PersonMapper(vertexFactory);
    }

    @Test
    public void testPersonMapper() throws ParseException {
        Tuple8<String, String, String, String, String, String, String, String> input =
                new Tuple8<>("123", "John Doe", "false", "2022-08-05 10:15:30", "Male", "1990-01-01", "USA", "New York");

        TemporalVertex result = personMapper.map(input);

        assertNotNull(result);
        assertEquals("Person", result.getLabel());
        assertEquals("123", result.getPropertyValue("ID").toString());
        assertEquals("John Doe", result.getPropertyValue("Name").toString());
        assertEquals("false", result.getPropertyValue("Is Blocked").toString());
        assertEquals("2022-08-05 10:15:30", result.getPropertyValue("CreateTime").toString());
        assertEquals("Male", result.getPropertyValue("Gender").toString());
        assertEquals("1990-01-01", result.getPropertyValue("Birthday").toString());
        assertEquals("USA", result.getPropertyValue("Country").toString());
        assertEquals("New York", result.getPropertyValue("City").toString());

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        long expectedValidFrom = dateFormat.parse("2022-08-05T10:15:30").getTime();
        assertEquals(expectedValidFrom, result.getValidFrom().longValue());
    }
}
