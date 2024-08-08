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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import static org.gradoop.importer.finbench.functions.HelperFunction.*;

/**
 * The EntitiesMapper class is responsible for providing suitable mapper for each entity type
 * to convert Tuple to TemporalVertex
 */

public class VertexMapper implements Serializable {

    private final PersonMapper personMapper;
    private final MediumMapper mediumMapper;
    private final AccountMapper accountMapper;
    private final LoanMapper loanMapper;

    private final CompanyMapper companyMapper;

    public VertexMapper(TemporalVertexFactory factory){
        this.personMapper = new PersonMapper(factory);
        this.mediumMapper = new MediumMapper(factory);
        this.accountMapper = new AccountMapper(factory);
        this.loanMapper = new LoanMapper(factory);
        this.companyMapper = new CompanyMapper(factory);
    }

    public PersonMapper getPersonMapper() {
        return personMapper;
    }
    public MediumMapper getMediumMapper() { return mediumMapper; }
    public AccountMapper getAccountMapper() { return accountMapper; }
    public LoanMapper getLoanMapper(){ return loanMapper; }
    public CompanyMapper getCompanyMapper(){return companyMapper;}


    public static class PersonMapper implements MapFunction<Tuple8<String, String, String, String, String, String, String, String>, TemporalVertex> {

        private TemporalVertexFactory factory;

        public PersonMapper(TemporalVertexFactory factory) {
            this.factory = factory;}

        /**
         * Maps a tuple representing a person to a TemporalVertex.
         *
         * @param record containing person information
         * @return the created TemporalVertex for the person
         * @throws ParseException if an error occurs during converting DateTime to UNIX
         */

        @Override
        public TemporalVertex map(Tuple8<String, String, String, String, String, String, String, String> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();

            propertiesMap.put("ID", record.f0);
            propertiesMap.put("Name", record.f1);
            propertiesMap.put("Is Blocked", record.f2);
            propertiesMap.put("CreateTime", record.f3);
            propertiesMap.put("Gender", record.f4);
            propertiesMap.put("Birthday", record.f5);
            propertiesMap.put("Country", record.f6);
            propertiesMap.put("City", record.f7);


            GradoopId id = GradoopId.get();

            TemporalVertex person = factory.initVertex(id, "Person", Properties.createFromMap(propertiesMap));
            person.setValidFrom(convertTimeToUnix(record.f3));

            return person;
        }
    }

    public static class MediumMapper implements MapFunction<Tuple6<String, String, String, String, String, String>, TemporalVertex>{
        private TemporalVertexFactory factory;

        public MediumMapper(TemporalVertexFactory factory) {
            this.factory = factory;
        }

        /**
         * Maps a tuple representing a medium to a TemporalVertex.
         *
         * @param record the tuple containing medium information
         * @return the created TemporalVertex for the medium
         * @throws ParseException if an error occurs during converting DateTime to UNIX
         */

        @Override
        public TemporalVertex map(Tuple6<String, String, String, String, String, String> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("ID", record.f0);
            propertiesMap.put("MediumType", record.f1);
            propertiesMap.put("IsBlocked", record.f2);
            propertiesMap.put("CreateTime", record.f3);
            propertiesMap.put("LastLogin", record.f4);
            propertiesMap.put("RiskLevel", record.f5);

            Long createTime = convertTimeToUnix(record.f3);

            GradoopId id = GradoopId.get();
            TemporalVertex medium = factory.initVertex(id, "Medium", Properties.createFromMap(propertiesMap));
            medium.setValidFrom(createTime);

            return medium;
        }
    }

    public static class AccountMapper implements MapFunction<Tuple10<String, String, Boolean, String, String, String, String, String, String, String>, TemporalVertex> {
        private TemporalVertexFactory factory;

        public AccountMapper(TemporalVertexFactory factory) {
            this.factory = factory;
        }

        /**
         * Maps a tuple representing an account to a TemporalVertex.
         *
         * @param record the tuple containing account information
         * @return the created TemporalVertex for the account
         * @throws ParseException if an error occurs during converting DateTime to UNIX
         */

        @Override
        public TemporalVertex map(Tuple10<String, String, Boolean, String, String, String, String, String, String, String> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("ID", record.f0);
            propertiesMap.put("CreateTime", record.f1);
            propertiesMap.put("IsBlocked", record.f2);
            propertiesMap.put("AccountType", record.f3);
            propertiesMap.put("Nickname", record.f4);
            propertiesMap.put("PhoneNumber", record.f5);
            propertiesMap.put("Email", record.f6);
            propertiesMap.put("FreqLoginType", record.f7);
            propertiesMap.put("LastLoginTime", record.f8);
            propertiesMap.put("AccountLevel", record.f9);

            Long createTime = convertTimeToUnix(record.f1);

            GradoopId id = GradoopId.get();

            TemporalVertex account = factory.initVertex(id, "Account", Properties.createFromMap(propertiesMap));
            account.setValidFrom(createTime);

            return account;
        }
    }

    public static class LoanMapper implements MapFunction<Tuple6<String, Double, Double, String, String, Double>, TemporalVertex>{
        private TemporalVertexFactory factory;

        public LoanMapper(TemporalVertexFactory factory) {
            this.factory = factory;
        }

        /**
         * Maps a tuple representing a loan to a TemporalVertex.
         *
         * @param record containing loan information
         * @return the created TemporalVertex for the loan
         * @throws ParseException if an error occurs during converting DateTime to UNIX
         */

        @Override
        public TemporalVertex map(Tuple6<String, Double, Double, String, String, Double> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("ID", record.f0);
            propertiesMap.put("Loan amount", record.f1);
            propertiesMap.put("Balance", record.f2);
            propertiesMap.put("Create Time", record.f3);
            propertiesMap.put("Loan usage", record.f4);
            propertiesMap.put("Interest Rate", record.f5);
            Long createTime = convertTimeToUnix(record.f3);

            GradoopId id = GradoopId.get();

            TemporalVertex loan = factory.initVertex(id, "Loan", Properties.createFromMap(propertiesMap));
            loan.setValidFrom(createTime);

            return loan;
        }
    }

    public static class CompanyMapper implements MapFunction<Tuple9<String,String,String,String,String,String,String,String,String>, TemporalVertex>{
        private TemporalVertexFactory factory;

        public CompanyMapper(TemporalVertexFactory factory){
            this.factory = factory;
        }

        /**
         * Maps a tuple representing a loan to a TemporalVertex.
         *
         * @param record containing loan information
         * @return the created TemporalVertex for the loan
         * @throws ParseException if an error occurs during converting DateTime to UNIX
         */

        @Override
        public TemporalVertex map(Tuple9<String,String,String,String,String,String,String,String,String> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("ID", record.f0);
            propertiesMap.put("Name", record.f1);
            propertiesMap.put("Create Time", record.f3);
            propertiesMap.put("Is Blocked", record.f2);
            propertiesMap.put("Country", record.f4);
            propertiesMap.put("City", record.f5);
            propertiesMap.put("Business", record.f6);
            propertiesMap.put("Description", record.f7);
            propertiesMap.put("URL", record.f8);
            Long createTime = convertTimeToUnix(record.f3);

            GradoopId id = GradoopId.get();

            TemporalVertex company = factory.initVertex(id, "Company", Properties.createFromMap(propertiesMap));
            company.setValidFrom(createTime);

            return company;
        }


    }
}
