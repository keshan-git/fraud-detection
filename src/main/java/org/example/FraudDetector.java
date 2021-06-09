/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private static final Logger LOG = LoggerFactory.getLogger( FraudDetector.class);

    private ValueState<FraudProcessState> state;

    @Override
    public void open( Configuration parameters ) throws Exception {
        state = getRuntimeContext( ).getState( new ValueStateDescriptor<>( "status", FraudProcessState.class ) );
    }

    @Override
    public void processElement( Transaction transaction, Context context, Collector<Alert> collector ) throws Exception {
        FraudProcessState currentStatus = state.value( );
        if ( currentStatus == null ) {
            currentStatus = new FraudProcessState( );
            currentStatus.setPrevious( transaction );
            state.update( currentStatus );
            LOG.info( "Initial state created for the account group id={}", transaction.getAccountId() );
            return;
        }

        LOG.debug( "Processing transaction, pre-{} current-{}", currentStatus.getPrevious( ), transaction );

        //fraud detector should output an alert for any account that makes a small transaction immediately followed by a large one
        if ( ( ( transaction.getTimestamp( ) - currentStatus.getPrevious( ).getTimestamp( ) ) < 1000 * ONE_MINUTE ) &&
                ( currentStatus.getPrevious( ).getAmount( ) < SMALL_AMOUNT ) && ( transaction.getAmount( ) > LARGE_AMOUNT ) ) {
            Alert alert = new Alert( );
            alert.setId( transaction.getAccountId( ) );
            collector.collect( alert );
            LOG.info( "Fraud event detected, alert triggered" );
        }

        currentStatus.setPrevious( transaction );
        state.update( currentStatus );
    }
}
