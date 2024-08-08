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
package org.gradoop.importer.finbench;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.importer.finbench.functions.EdgeReader;
import org.gradoop.importer.finbench.functions.VertexReader;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * The DataSourceExtractor class is responsible for reading vertex and edge data from CSV files
 * and generate DataSet as DataSource for graphs.
 */

public class FinBenchTemporalDataSource implements TemporalDataSource {

    private final String inputPath;

    private final TemporalGradoopConfig config;

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
    public FinBenchTemporalDataSource(String inputPath, ExecutionEnvironment env, TemporalGradoopConfig config){
        this.edgeReader = new EdgeReader(env);
        this.entitiesReader = new VertexReader(env);
        this.inputPath = inputPath;
        this.config = config;
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

    @Override
    public TemporalGraph getTemporalGraph() throws IOException {
        DataSet<TemporalVertex> accountVertices = entitiesReader.readingAccount(getPath(inputPath, ACCOUNT_PATH));
        DataSet<TemporalVertex> companyVertices = entitiesReader.readingCompany(getPath(inputPath, COMPANY_PATH));
        DataSet<TemporalVertex> mediumVertices = entitiesReader.readingMedium(getPath(inputPath, MEDIUM_PATH));
        DataSet<TemporalVertex> personVertices = entitiesReader.readingPerson(getPath(inputPath, PERSON_PATH));
        DataSet<TemporalVertex> loanVertices = entitiesReader.readingLoan(getPath(inputPath, LOAN_PATH));


        DataSet<TemporalEdge> edges = edgeReader.readingRepay(getPath(inputPath, ACCOUNT_REPAY_LOAN_PATH), accountVertices, loanVertices)
          .union(edgeReader.readingTransfer(getPath(inputPath, ACCOUNT_TRANSFER_ACCOUNT_PATH), accountVertices, accountVertices))
          .union(edgeReader.readingWithdraw(getPath(inputPath, ACCOUNT_WITHDRAW_ACCOUNT_PATH), accountVertices, accountVertices))
          .union(edgeReader.readingApply(getPath(inputPath, COMPANY_APPLY_LOAN_PATH), companyVertices, loanVertices))
          .union(edgeReader.readingInvest(getPath(inputPath, COMPANY_INVEST_COMPANY_PATH), companyVertices, companyVertices))
          .union(edgeReader.readingGuarantee(getPath(inputPath, COMPANY_GUARANTEE_COMPANY_PATH), companyVertices, companyVertices))
          .union(edgeReader.readingOwn(getPath(inputPath, COMPANY_OWN_ACCOUNT_PATH), companyVertices, accountVertices))
          .union(edgeReader.readingDeposit(getPath(inputPath, LOAN_DEPOSIT_ACCOUNT_PATH), loanVertices, accountVertices))
          .union(edgeReader.readingSignIn(getPath(inputPath, MEDIUM_SIGNIN_ACCOUNT_PATH), mediumVertices, accountVertices))
          .union(edgeReader.readingApply(getPath(inputPath, PERSON_APPLY_LOAN_PATH), personVertices, loanVertices))
          .union(edgeReader.readingGuarantee(getPath(inputPath, PERSON_GUARANTEE_PERSON_PATH), personVertices, personVertices))
          .union(edgeReader.readingInvest(getPath(inputPath, PERSON_INVEST_COMPANY_PATH), personVertices, companyVertices))
          .union(edgeReader.readingOwn(getPath(inputPath, PERSON_OWN_ACCOUNT_PATH), personVertices, accountVertices));

        DataSet<TemporalVertex> vertices = accountVertices.union(companyVertices).union(mediumVertices).union(personVertices).union(loanVertices);

        return config.getTemporalGraphFactory().fromDataSets(vertices, edges);
    }

    @Override
    public TemporalGraphCollection getTemporalGraphCollection() throws IOException {
        return config.getTemporalGraphCollectionFactory().fromGraph(getTemporalGraph());
    }
}