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


    public static class PersonMapper implements MapFunction<Tuple8<Long, String, Boolean, String, String, String, String, String>, TemporalVertex> {

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
        public TemporalVertex map(Tuple8<Long, String, Boolean, String, String, String, String, String> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();

            propertiesMap.put("id", record.f0);
            propertiesMap.put("name", record.f1);
            propertiesMap.put("isBlocked", record.f2);
            propertiesMap.put("createTime", record.f3);
            propertiesMap.put("gender", record.f4);
            propertiesMap.put("birthday", record.f5);
            propertiesMap.put("country", record.f6);
            propertiesMap.put("city", record.f7);


            GradoopId id = GradoopId.get();

            TemporalVertex person = factory.initVertex(id, "Person", Properties.createFromMap(propertiesMap));
            person.setValidFrom(convertTimeToUnix(record.f3));

            return person;
        }
    }

    public static class MediumMapper implements MapFunction<Tuple6<Long, String, Boolean, String, String, String>, TemporalVertex>{
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
        public TemporalVertex map(Tuple6<Long, String, Boolean, String, String, String> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("id", record.f0);
            propertiesMap.put("type", record.f1);
            propertiesMap.put("isBlocked", record.f2);
            propertiesMap.put("createTime", record.f3);
            propertiesMap.put("lastLoginTime", record.f4);
            propertiesMap.put("riskLevel", record.f5);

            Long createTime = convertTimeToUnix(record.f3);

            GradoopId id = GradoopId.get();
            TemporalVertex medium = factory.initVertex(id, "Medium", Properties.createFromMap(propertiesMap));
            medium.setValidFrom(createTime);

            return medium;
        }
    }

    public static class AccountMapper implements MapFunction<Tuple10<Long, String, Boolean, String, String, String, String, String, String, String>, TemporalVertex> {
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
        public TemporalVertex map(Tuple10<Long, String, Boolean, String, String, String, String, String, String, String> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("id", record.f0);
            propertiesMap.put("createTime", record.f1);
            propertiesMap.put("isBlocked", record.f2);
            propertiesMap.put("type", record.f3);
            propertiesMap.put("nickname", record.f4);
            propertiesMap.put("phoneNumber", record.f5);
            propertiesMap.put("email", record.f6);
            propertiesMap.put("freqLoginType", record.f7);
            propertiesMap.put("lastLoginTime", record.f8);
            propertiesMap.put("accountLevel", record.f9);

            Long createTime = convertTimeToUnix(record.f1);

            GradoopId id = GradoopId.get();

            TemporalVertex account = factory.initVertex(id, "Account", Properties.createFromMap(propertiesMap));
            account.setValidFrom(createTime);

            return account;
        }
    }

    public static class LoanMapper implements MapFunction<Tuple6<Long, Double, Double, String, String, Double>, TemporalVertex>{
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
        public TemporalVertex map(Tuple6<Long, Double, Double, String, String, Double> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("id", record.f0);
            propertiesMap.put("loanAmount", record.f1);
            propertiesMap.put("balance", record.f2);
            propertiesMap.put("createTime", record.f3);
            propertiesMap.put("usage", record.f4);
            propertiesMap.put("interestRate", record.f5);
            Long createTime = convertTimeToUnix(record.f3);

            GradoopId id = GradoopId.get();

            TemporalVertex loan = factory.initVertex(id, "Loan", Properties.createFromMap(propertiesMap));
            loan.setValidFrom(createTime);

            return loan;
        }
    }

    public static class CompanyMapper implements MapFunction<Tuple9<Long,String,Boolean,String,String,String,String,String,String>, TemporalVertex>{
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
        public TemporalVertex map(Tuple9<Long,String,Boolean,String,String,String,String,String,String> record) throws ParseException {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("id", record.f0);
            propertiesMap.put("name", record.f1);
            propertiesMap.put("createTime", record.f3);
            propertiesMap.put("isBlocked", record.f2);
            propertiesMap.put("country", record.f4);
            propertiesMap.put("city", record.f5);
            propertiesMap.put("business", record.f6);
            propertiesMap.put("description", record.f7);
            propertiesMap.put("url", record.f8);
            Long createTime = convertTimeToUnix(record.f3);

            GradoopId id = GradoopId.get();

            TemporalVertex company = factory.initVertex(id, "Company", Properties.createFromMap(propertiesMap));
            company.setValidFrom(createTime);

            return company;
        }


    }
}
