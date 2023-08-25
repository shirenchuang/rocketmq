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
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.net.InetAddress;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static final String CONSUMER_GROUP = "please_rename_unique_group_name_4";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTest";


    public static void testAllByName(){
        String hostname = "rocketmq-srv.cainiao.test";

        try {
            InetAddress[] addresses = InetAddress.getAllByName(hostname);
            for (InetAddress address : addresses) {
                System.out.println(address.getHostAddress());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws MQClientException {

        //testAllByName();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("szz_consumer_group",true);
        //consumer.setNamesrvAddr("rocketmq-srv.cainiao.test:9876");
        //consumer.setNamesrvAddr("szzdzhp.com:9876;szzdzhp.com:9877");
        consumer.setNamesrvAddr("http://rocketmq-srv.cainiao.test:9876; ");

        consumer.subscribe("sutee_mq_rebalance", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s consumer1 Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //consumer.setInstanceName("rocketmq_cn_default_instance2222");

        consumer.setPullBatchSize(33);
        consumer.start();

    }

    public static void main1(String[] args) throws MQClientException {

        String topic = "sutee_mq_rebalance";
        String tag = "Tag-SZZ";
        String groupName = "szz_consumer_group";

        AllocateMachineRoomNearby nearby = new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), new SzzMachineRoomResolver());

        /*
         * Instantiate with specified consumer group name.
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName,true);
        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s consumer1 Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
       // consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setInstanceName("szz_consumer1|szz1|");
        System.out.printf("Consumer1 Started.%n");
       // consumer.setAllocateMessageQueueStrategy(nearby);

        // 开启服务端重平衡
        //consumer.setClientRebalance(false);
        consumer.start();

        consumer.getDefaultMQPushConsumerImpl().suspend();

        /*
         * Instantiate with specified consumer group name.
         */
        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(groupName,true);
        consumer2.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        consumer2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer2.subscribe(topic, "*");
        consumer2.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s consumer2 Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

       // consumer2.setAllocateMessageQueueStrategy(nearby);

        consumer2.setInstanceName("szz_consumer2|szz2|");
        //consumer2.setMessageModel(MessageModel.BROADCASTING);

        //consumer2.setClientRebalance(false);
        consumer2.start();
        consumer2.getDefaultMQPushConsumerImpl().suspend();


        DefaultMQPushConsumer consumer3 = new DefaultMQPushConsumer(groupName,true);
        consumer3.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        consumer3.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer3.subscribe(topic, "*");
        consumer3.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s consumer3 Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

       // consumer3.setAllocateMessageQueueStrategy(nearby);
        consumer3.setInstanceName("szz_consumer3|szz3|");
        //consumer2.setMessageModel(MessageModel.BROADCASTING);

        //consumer3.setClientRebalance(false);
        consumer3.start();
        consumer3.getDefaultMQPushConsumerImpl().suspend();
        consumer3.getDefaultMQPushConsumerImpl().resume();

        System.out.printf("Consumer3 Started.%n");
    }


    public static class SzzMachineRoomResolver implements AllocateMachineRoomNearby.MachineRoomResolver {

        @Override
        public String brokerDeployIn(MessageQueue messageQueue) {
            // 解析该队列是在哪个机房

            // 自定义 BrokerName格式: 机房_BrokerName；  例如： room1_Broker-a
            return messageQueue.getBrokerName().split("_")[0];
        }

        @Override
        public String consumerDeployIn(String clientID) {
            // 解析消费者客户端是哪个机房

            // 一般clientID格式:  ClientIP@InstanceName@xxx;  所以①.可以通过IP来区分机房; ②.通过设置InstanName属性来标记所属机房；但是这种方式不太友好,不推荐
                // 这里自定义格式InstanceName=实例名|机房|  例如：127.0.0.1@szz_consumer|机房1|
            return getMiddleString(clientID);
        }

        public static String getMiddleString(String input) {
            int start = input.indexOf("|") + 1;
            int end = input.indexOf("|", start);
            if (start > 0 && end > 0) {
                return input.substring(start, end);
            }
            return "";
        }
    }
}
