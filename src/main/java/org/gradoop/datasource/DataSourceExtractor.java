package org.gradoop.datasource;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.edge.EdgeReader;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.vertex.VertexReader;

import java.nio.file.Paths;

/**
 * The DataSourceExtractor class is responsible for reading vertex and edge data from CSV files
 * and generate DataSet as DataSource for graphs.
 */

public class DataSourceExtractor {

    private static final String COMPANY_PATH = "/Company.csv";
    private static final String ACCOUNT_PATH = "/Account.csv";
    private static final String LOAN_PATH = "/Loan.csv";
    private static final String MEDIUM_PATH = "/Medium.csv";
    private static final String PERSON_PATH = "/Person.csv";

    private static final String ACCOUNT_REPAY_LOAN_PATH = "/AccountRepayLoan.csv";
    private static final String ACCOUNT_TRANSFER_ACCOUNT_PATH = "/AccountTransferAccount.csv";
    private static final String ACCOUNT_WITHDRAW_ACCOUNT_PATH = "/AccountWithdrawAccount.csv";
    private static final String COMPANY_APPLY_LOAN_PATH = "/CompanyApplyLoan.csv";
    private static final String COMPANY_INVEST_COMPANY_PATH = "/CompanyInvestCompany.csv";
    private static final String COMPANY_GUARANTEE_COMPANY_PATH = "/CompanyGuaranteeCompany.csv";
    private static final String COMPANY_OWN_ACCOUNT_PATH = "/CompanyOwnAccount.csv";
    private static final String LOAN_DEPOSIT_ACCOUNT_PATH = "/LoanDepositAccount.csv";
    private static final String MEDIUM_SIGNIN_ACCOUNT_PATH = "/MediumSignInAccount.csv";
    private static final String PERSON_APPLY_LOAN_PATH = "/PersonApplyLoan.csv";
    private static final String PERSON_GUARANTEE_PERSON_PATH = "/PersonGuaranteePerson.csv";
    private static final String PERSON_INVEST_COMPANY_PATH = "/PersonInvestCompany.csv";
    private static final String PERSON_OWN_ACCOUNT_PATH = "/PersonOwnAccount.csv";


    EdgeReader edgeReader;
    VertexReader entitiesReader;
    public DataSourceExtractor(ExecutionEnvironment env){
        this.edgeReader = new EdgeReader(env);
        this.entitiesReader = new VertexReader(env);
    }

    /**
     * Reads vertex and edge data from CSV files located in the specified input directory.
     *
     * @param inputDirectory the directory containing the CSV files
     * @return a tuple containing DataSets of all vertices and edges
     */

    public Tuple2<DataSet<TemporalVertex>, DataSet<TemporalEdge>> readingFinbench (String inputDirectory) {
        DataSet<TemporalVertex> accountVertices = entitiesReader.readingAccount(getPath(inputDirectory, ACCOUNT_PATH));
        DataSet<TemporalVertex> companyVertices = entitiesReader.readingCompany(getPath(inputDirectory, COMPANY_PATH));
        DataSet<TemporalVertex> mediumVertices = entitiesReader.readingMedium(getPath(inputDirectory, MEDIUM_PATH));
        DataSet<TemporalVertex> personVertices = entitiesReader.readingPerson(getPath(inputDirectory, PERSON_PATH));
        DataSet<TemporalVertex> loanVertices = entitiesReader.readingLoan(getPath(inputDirectory, LOAN_PATH));


        DataSet<TemporalEdge> edges = edgeReader.readingRepay(getPath(inputDirectory, ACCOUNT_REPAY_LOAN_PATH), accountVertices, loanVertices)
                .union(edgeReader.readingTransfer(getPath(inputDirectory, ACCOUNT_TRANSFER_ACCOUNT_PATH), accountVertices, accountVertices))
                .union(edgeReader.readingWithdraw(getPath(inputDirectory, ACCOUNT_WITHDRAW_ACCOUNT_PATH), accountVertices, accountVertices))
                .union(edgeReader.readingApply(getPath(inputDirectory, COMPANY_APPLY_LOAN_PATH), companyVertices, loanVertices))
                .union(edgeReader.readingInvest(getPath(inputDirectory, COMPANY_INVEST_COMPANY_PATH), companyVertices, companyVertices))
                .union(edgeReader.readingGuarantee(getPath(inputDirectory, COMPANY_GUARANTEE_COMPANY_PATH), companyVertices, companyVertices))
                .union(edgeReader.readingOwn(getPath(inputDirectory, COMPANY_OWN_ACCOUNT_PATH), companyVertices, accountVertices))
                .union(edgeReader.readingDeposit(getPath(inputDirectory, LOAN_DEPOSIT_ACCOUNT_PATH), loanVertices, accountVertices))
                .union(edgeReader.readingSignIn(getPath(inputDirectory, MEDIUM_SIGNIN_ACCOUNT_PATH), mediumVertices, accountVertices))
                .union(edgeReader.readingApply(getPath(inputDirectory, PERSON_APPLY_LOAN_PATH), personVertices, loanVertices))
                .union(edgeReader.readingGuarantee(getPath(inputDirectory, PERSON_GUARANTEE_PERSON_PATH), personVertices, personVertices))
                .union(edgeReader.readingInvest(getPath(inputDirectory, PERSON_INVEST_COMPANY_PATH), personVertices, companyVertices))
                .union(edgeReader.readingOwn(getPath(inputDirectory, PERSON_OWN_ACCOUNT_PATH), personVertices, accountVertices));

        DataSet<TemporalVertex> vertices = accountVertices.union(companyVertices).union(mediumVertices).union(personVertices).union(loanVertices);

        return new Tuple2<>(vertices,edges);
    }

    /**
     * Constructs the full path to a specficfile given the input directory and file name.
     *
     * @param inputDirectory the directory containing the file
     * @param fileName the name of the file
     * @return path to the needed file
     */

    public String getPath(String inputDirectory, String fileName){
        return Paths.get(inputDirectory,fileName).toString();
    }
}