package org.gradoop.vertex;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;

import java.io.Serializable;


/**
 * The EntitiesReader class is responsible for reading CSV files containing entity data and mapping them
 * to TemporalVertex objects using suitable mappers.
 */

public class VertexReader implements Serializable {

    private transient ExecutionEnvironment env;
    private TemporalVertexFactory factory;
    private VertexMapper mappers;

    public VertexReader(ExecutionEnvironment env) {
        this.env = env;
        this.factory = new TemporalVertexFactory();
        this.mappers = new VertexMapper(this.factory);
    }

    /**
     * Reads a CSV file containing person data and maps it to a DataSet of TemporalVertex objects.
     *
     * @param filePath the path to the CSV file.
     * @return a DataSet of TemporalVertex objects representing persons.
     */

    public DataSet<TemporalVertex> readingPerson(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .map(mappers.getPersonMapper())
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }

    /**
     * Reads a CSV file containing loan data and maps it to a DataSet of TemporalVertex objects.
     *
     * @param filePath the path to the CSV file.
     * @return a DataSet of TemporalVertex objects representing loans.
     */

    public DataSet<TemporalVertex> readingLoan(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, Double.class, Double.class, String.class, String.class, Double.class)
                .map(mappers.getLoanMapper())
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }

    /**
     * Reads a CSV file containing company data and maps it to a DataSet of TemporalVertex objects.
     *
     * @param filePath the path to the CSV file.
     * @return a DataSet of TemporalVertex objects representing companies.
     */

    public DataSet<TemporalVertex> readingCompany(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .map(mappers.getCompanyMapper())
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }

    /**
     * Reads a CSV file containing account data and maps it to a DataSet of TemporalVertex objects.
     *
     * @param filePath the path to the CSV file.
     * @return a DataSet of TemporalVertex objects representing accounts.
     */

    public DataSet<TemporalVertex> readingAccount(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, Boolean.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .map(mappers.getAccountMapper())
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }

    /**
     * Reads a CSV file containing medium data and maps it to a DataSet of TemporalVertex objects.
     *
     * @param filePath the path to the CSV file.
     * @return a DataSet of TemporalVertex objects representing mediums.
     */

    public DataSet<TemporalVertex> readingMedium(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class, String.class, String.class)
                .map(mappers.getMediumMapper())
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }
}
