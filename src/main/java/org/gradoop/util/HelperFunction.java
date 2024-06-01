package org.gradoop.util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HelperFunction {

    /**
     * Converts a timestamp string to a Unix time in milliseconds.
     *
     * @param timestamp the timestamp string in the format "yyyy-MM-dd HH:mm:ss.SSS" (up to 3 digits of milliseconds).
     * @return the Unix time in milliseconds.
     * @throws ParseException if the timestamp string cannot be parsed.
     */
    public static long convertTimeToUnix(String timestamp) throws ParseException {
        int millisIndex = timestamp.lastIndexOf('.');
        int missingZeros = 3 - (timestamp.length() - millisIndex - 1);

        if (millisIndex == -1) {
            timestamp = timestamp + ".000";
        } else if (missingZeros > 0) {
            StringBuilder zeros = new StringBuilder();
            for (int i = 0; i < missingZeros; i++) {
                zeros.append('0');
            }
            timestamp = timestamp.substring(0, millisIndex + 1) + zeros + timestamp.substring(millisIndex + 1);
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date parsedDate = dateFormat.parse(timestamp);
        return parsedDate.getTime();
    }

    /**
     * Generates a DataSet of ID pairs (FinBenchID, GradoopID) from a DataSet of TemporalVertex objects.
     *
     * @param vertices the DataSet of TemporalVertex objects.
     * @return a DataSet of tuples containing the orignial FinBenchID and the corresponding GradoopId.
     */
    public static DataSet<Tuple2<String, GradoopId>>  generateIdPairs(DataSet<TemporalVertex> vertices) {
        return vertices
                .map(vertex -> new Tuple2<>(vertex.getPropertyValue("ID").toString(), vertex.getId()))
                .returns(new TypeHint<Tuple2<String, GradoopId>>() {})
                .distinct();
    }

}
