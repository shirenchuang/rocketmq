/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MQClientInstanceTest {
    private MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();

    @Before
    public void init() throws Exception {
        FieldUtils.writeDeclaredField(mqClientInstance, "brokerAddrTable", brokerAddrTable, true);
    }

    @Test
    public void testTopicRouteData2TopicPublishInfo() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<>());
        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);

        TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);

        assertThat(topicPublishInfo.isHaveTopicRouterInfo()).isFalse();
        assertThat(topicPublishInfo.getMessageQueueList().size()).isEqualTo(4);
    }

    @Test
    public void testFindBrokerAddressInSubscribe() {
        // dledger normal case
        String brokerName = "BrokerA";
        HashMap<Long, String> addrMap = new HashMap<>();
        addrMap.put(0L, "127.0.0.1:10911");
        addrMap.put(1L, "127.0.0.1:10912");
        addrMap.put(2L, "127.0.0.1:10913");
        brokerAddrTable.put(brokerName, addrMap);
        long brokerId = 1;
        FindBrokerResult brokerResult = mqClientInstance.findBrokerAddressInSubscribe(brokerName, brokerId, false);
        assertThat(brokerResult).isNotNull();
        assertThat(brokerResult.getBrokerAddr()).isEqualTo("127.0.0.1:10912");
        assertThat(brokerResult.isSlave()).isTrue();

        // dledger case, when node n0 was voted as the leader
        brokerName = "BrokerB";
        HashMap<Long, String> addrMapNew = new HashMap<>();
        addrMapNew.put(0L, "127.0.0.1:10911");
        addrMapNew.put(2L, "127.0.0.1:10912");
        addrMapNew.put(3L, "127.0.0.1:10913");
        brokerAddrTable.put(brokerName, addrMapNew);
        brokerResult = mqClientInstance.findBrokerAddressInSubscribe(brokerName, brokerId, false);
        assertThat(brokerResult).isNotNull();
        assertThat(brokerResult.getBrokerAddr()).isEqualTo("127.0.0.1:10912");
        assertThat(brokerResult.isSlave()).isTrue();
    }

    @Test
    public void testRegisterProducer() {
        boolean flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterProducer(group);
        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testRegisterConsumer() throws RemotingException, InterruptedException, MQBrokerException {
        boolean flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();
    }


    @Test
    public void testConsumerRunningInfoWhenConsumersIsEmptyOrNot() throws RemotingException, InterruptedException, MQBrokerException {
        MQConsumerInner mockConsumerInner = mock(MQConsumerInner.class);
        ConsumerRunningInfo mockConsumerRunningInfo = mock(ConsumerRunningInfo.class);
        when(mockConsumerInner.consumerRunningInfo()).thenReturn(mockConsumerRunningInfo);
        when(mockConsumerInner.consumeType()).thenReturn(ConsumeType.CONSUME_PASSIVELY);
        Properties properties = new Properties();
        when(mockConsumerRunningInfo.getProperties()).thenReturn(properties);
        mqClientInstance.unregisterConsumer(group);

        ConsumerRunningInfo runningInfo = mqClientInstance.consumerRunningInfo(group);
        assertThat(runningInfo).isNull();
        boolean flag = mqClientInstance.registerConsumer(group, mockConsumerInner);
        assertThat(flag).isTrue();

        runningInfo = mqClientInstance.consumerRunningInfo(group);
        assertThat(runningInfo).isNotNull();
        assertThat(mockConsumerInner.consumerRunningInfo().getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).isNotNull();

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testRegisterAdminExt() {
        boolean flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterAdminExt(group);
        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isTrue();
    }


    @Test
    public void testOnlyRegisterProducer() throws MQClientException {

        DefaultMQProducer producer = new DefaultMQProducer("szz_producer");
        producer.setInstanceName("szz_producer_instanceName");
        producer.start();

        // alread created
        MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(producer);

        // only producer shouldn't start inner DefaultMQProducer and pullRequest
        ServiceState serviceState = mqClientInstance.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceState).isEqualTo(ServiceState.CREATE_JUST);

        producer.shutdown();

        ServiceState serviceStateAfter = mqClientInstance.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceStateAfter).isEqualTo(ServiceState.SHUTDOWN_ALREADY);

        ServiceState producerServiceState = producer.getDefaultMQProducerImpl().getServiceState();
        assertThat(producerServiceState).isEqualTo(ServiceState.SHUTDOWN_ALREADY);

    }

    @Test
    public void testOnlyRegisterConsumer() throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("szz_consumer");
        consumer.setInstanceName("szz_consumer_instanceName");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        // already created
        MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(consumer);

        ServiceState serviceState = mqClientInstance.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceState).isEqualTo(ServiceState.RUNNING);

        consumer.shutdown();

        ServiceState serviceStateAfter = mqClientInstance.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceStateAfter).isEqualTo(ServiceState.SHUTDOWN_ALREADY);

    }


    @Test
    public void testBothProducerAndConsumer() throws MQClientException {

        DefaultMQProducer producer = new DefaultMQProducer("szz_producer_3");
        producer.setInstanceName("szz_producerAndconsumer_instanceName");
        producer.start();

        // alread created
        MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(producer);

        // only producer shouldn't start inner DefaultMQProducer and pullRequest
        ServiceState serviceState = mqClientInstance.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceState).isEqualTo(ServiceState.CREATE_JUST);


        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("szz_consumer_3");
        consumer.setInstanceName("szz_producerAndconsumer_instanceName");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        // already created
        MQClientInstance mqClientInstance2 = MQClientManager.getInstance().getOrCreateMQClientInstance(consumer);

        // same instanceName should use same one MQClientInstance
        assertThat(mqClientInstance).isEqualTo(mqClientInstance2);

        // consumer started should start inner DefaultMqProducer and pullRequestService
        ServiceState serviceStateAfter = mqClientInstance2.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState();
        assertThat(serviceStateAfter).isEqualTo(ServiceState.RUNNING);

    }


}
