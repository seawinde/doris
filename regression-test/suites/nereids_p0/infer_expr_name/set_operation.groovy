// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("set_operation") {

    def queryResult1 = sql """
    explain analyzed plan
    SELECT 'STORE', 
           date_add(ss_sold_date_sk, INTERVAL 2 DAY),
           ss_item_sk as item_sk,
           ss_sales_price as sales_price,
           'STORE()##'
    FROM store_sales
    UNION ALL
    SELECT 'WEB',
           ws_sold_date_sk as date_sk,
           ws_item_sk as item_sk,
           ws_sales_price as sales_price,
           ''
    FROM web_sales
    UNION ALL
    SELECT 'CATALOG' as channel,
           cs_sold_date_sk as date_sk,
           cs_item_sk as item_sk,
           cs_sales_price as sales_price,
           ''
    FROM catalog_sales;
    """

    def topPlan1 = queryResult1[0][0].toString()
    assertTrue(topPlan1.contains("LogicalResultSink"))
    assertTrue(topPlan1.contains(""))


    def queryResult2 = sql """
    """

    def topPlan2 = queryResult2[0][0].toString()
    assertTrue(topPlan2.contains("LogicalResultSink"))
    assertTrue(topPlan2.contains(""))


    def queryResult3 = sql """
    """

    def topPlan3 = queryResult3[0][0].toString()
    assertTrue(topPlan3.contains("LogicalResultSink"))
    assertTrue(topPlan3.contains(""))

}

