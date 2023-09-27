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
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMachineRoomNearby;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;

import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static final String CONSUMER_GROUP = "please_rename_unique_group_name_4";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTest";


    public static void testAllByName() {
        String hostname = "11.159.145.195";

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

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(null,"szz_consumer_group_02", new SzzConsumerRpcHook());
        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        consumer.setInstanceName("szz_instance");
        consumer.subscribe("test", "*");
        consumer.registerMessageListener((MessageListenerOrderly) (msg, context) -> {
           // System.out.printf(" ----- %s 消费消息: %s  本批次大小: %s   ------ ", Thread.currentThread().getName(), msg, msg.size());

            for (int i = 0; i < msg.size(); i++) {
                java.lang.String s = java.lang.String.valueOf(msg.get(i).getBody());
                System.out.println("body"+ s +", MSG: " + msg.get(i) );
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });

        consumer.getDefaultMQPushConsumerImpl().registerFilterMessageHook(new SzzConsumerFilterHooks());

        consumer.registerConsumeMessageHook(new SzzConsumeMessageHooks());


        consumer.setPullBatchSize(5);
        // 一次处理消息的数量大小
        consumer.setConsumeMessageBatchMaxSize(1);
        ///consumer.registerConsumeMessageHook(new SzzConsumerMessageHook());
        // 50秒持久化一次
        consumer.setPersistConsumerOffsetInterval(50*1000);
        consumer.start();

    }




    public static void consumerConcurrently(String[] args) throws MQClientException {

        //testAllByName();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("szz_consumer_group_01", true);
        //consumer.setNamesrvAddr("szzdzhp.com:9876;szzdzhp.com:9877");
        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        consumer.subscribe("BatchTest_szz", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf(" ----- %s 消费消息: %s  本批次大小: %s   ------ ", Thread.currentThread().getName(), msg, msg.size());

            for (int i = 0; i < msg.size(); i++) {
                System.out.println("第 " + i + " 条消息, MSG: " + msg.get(i));
                // 延迟等级5 = 延迟1分钟;
                //context.setDelayLevelWhenNextConsume(5);

                // 或者你也可以根据重试的次数来递增延迟级别
                //context.setDelayLevelWhenNextConsume(3 + msg.get(i).getReconsumeTimes());
            }
            //context.setAckIndex(2);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });System.out.println();
        //consumer.setInstanceName("rocketmq_cn_default_instance2222");
        //
        consumer.setPullBatchSize(6);
        // 一次处理消息的数量大小
        consumer.setConsumeMessageBatchMaxSize(1);
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
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName, true);
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
        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(groupName, true);
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


        DefaultMQPushConsumer consumer3 = new DefaultMQPushConsumer(groupName, true);
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


    public static class SzzConsumerRpcHook implements RPCHook{

        @Override
        public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
            request.setRemark("test RpcHook");

        }

        @Override
        public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
            // 消费的时候，是异步请求，异步请求不会执行这里
        }
    }


    public static class SzzConsumerFilterHooks implements FilterMessageHook{
        @Override
        public String hookName() {
            return "SzzConsumerFilterHooks";
        }
        @Override
        public void filterMessage(FilterMessageContext context) {
            List<MessageExt> list =  context.getMsgList();
            // commitLogOffset要大于2000 我才想消费
            List<MessageExt> toRmv = list.stream().filter(msg -> msg.getCommitLogOffset() <= 2000).collect(Collectors.toList());
            list.removeAll(toRmv);
        }
    }

    public static class SzzConsumeMessageHooks implements ConsumeMessageHook{

        @Override
        public String hookName() {
            return "SzzConsumeMessageHooks";
        }

        @Override
        public void consumeMessageBefore(ConsumeMessageContext context) {

            List<MessageExt> msgList = context.getMsgList();
            if(msgList.get(0).getReconsumeTimes() > 10){
                System.out.println("消息："+msgList.get(0).getMsgId()+"都重试超过10次了,检查下什么问题吧老铁");
            }
        }

        @Override
        public void consumeMessageAfter(ConsumeMessageContext context) {
            if(!context.isSuccess()){
                System.out.println("消费失败了");
            }
        }
    }
}
