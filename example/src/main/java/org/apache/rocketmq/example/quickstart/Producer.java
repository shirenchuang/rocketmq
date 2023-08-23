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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.collections.MapUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.*;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {

    /**
     * The number of produced messages.
     */
    public static final int MESSAGE_COUNT = 1000;
    public static final String PRODUCER_GROUP = "szz_producer_group_name";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String DAILY_NAMESRVADDR = "rocketmq-srv.cainiao.test:9876";
    public static final String TOPIC = "TopicTest";
    public static final String TAG = "TagA";

    public static void main1(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        /*
         * Specify name server addresses.
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         *  producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);


        /*
         * Launch the instance.
         */
        producer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                Message msg = new Message(TOPIC /* Topic */,
                        TAG /* Tag */,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                /*
                 * Call send message to deliver message to one of brokers.
                 */
                SendResult sendResult = producer.send(msg);
                /*
                 * There are different ways to send message, if you don't care about the send result,you can use this way
                 * {@code
                 * producer.sendOneway(msg);
                 * }
                 */

                /*
                 * if you want to get the send result in a synchronize way, you can use this send method
                 * {@code
                 * SendResult sendResult = producer.send(msg);
                 * System.out.printf("%s%n", sendResult);
                 * }
                 */

                /*
                 * if you want to get the send result in a asynchronize way, you can use this send method
                 * {@code
                 *
                 *  producer.send(msg, new SendCallback() {
                 *  @Override
                 *  public void onSuccess(SendResult sendResult) {
                 *      // do something
                 *  }
                 *
                 *  @Override
                 *  public void onException(Throwable e) {
                 *      // do something
                 *  }
                 *});
                 *
                 *}
                 */

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is no longer in use.
         */
        producer.shutdown();
    }


    public static void main(String[] args) {
        try {
            //sendSyncMsg(args);
            //sendSyncMsgTestHooks(args);
            //sendSyncMsgSelector(args);
            sendMsg2Producer(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sendBatchMsg() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("group_batch");
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();

        List<Message> list = new ArrayList<>();
        Message msg1 = new Message("batch_topic","tag","key","批量消息1".getBytes());
        Message msg2 = new Message("batch_topic","tag","key","批量消息2".getBytes());
        list.add(msg1);
        list.add(msg2);
        producer.send(list);

    }


    public static void sendOrderlyMsg() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("group_orderly");
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();

        Message msg = new Message("orderly_topic","tag","key","顺序消息啊".getBytes());


    }


    public static void sendMsg2Producer(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {




        DefaultMQProducer producer1 = new DefaultMQProducer("group1");
        //producer1.setNamesrvAddr("rocketmq-srv.cainiao.test:9876");
        producer1.setNamesrvAddr("127.0.0.1:9876");
        producer1.start();




        try {
            producer1.send(new Message("sutee_mq_rebalance","TAG2","hello DEFAULT_NAMESRVADDR".getBytes()));
            System.out.println("producer1:success");

        }catch (Exception e){

            System.out.println("producer1:"+e.getMessage());
        }


    }

    /**
     * 发送同步消息，同步消息没有callback，只有异步消息才有
     *
     * @throws MQClientException
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     */
    public static void sendSyncMsg(String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException {


        // 第四种方式
        // System.setProperty("rocketmq.namesrv.domain","127.0.0.1:7003");
        //System.setProperty("rocketmq.namesrv.domain.subgroup","getnsaddr");


        //String topic = "SZZ-SyncMsg";
        String topic = "szz_t6";
        String tag = "Tag-SZZ";
        String groupName = "szz_producer_group";
        // 设置自定义Hook 和 开启消息轨迹
        DefaultMQProducer producer = new DefaultMQProducer(groupName, new SzzProducerRPCHook(), true, null);
        // 可以通过  系统变量rocketmq.namesrv.addr > 环境变量：NAMESRV_ADDR 设置 ；
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        //producer.setNamesrvAddr("http://jmenv.tbsite.net:8080/rocketmq/nsaddr");


        //producer.setUnitName("szz_unitname");

        // 如果设置了 命名空间的话, 最终的ProducerGroup为【${namespace}%groupName】 例如： szz_daily%szz_producer_group
        //producer.setNamespace("szz_daily");
        // 设置生产者客户端实例名称; 可以通过系统属性`rocketmq.client.name` 设置，没有设置的话默认DEFAULT; 但是启动的时候判断如果是DEFAULT，则将它改成：PID@时间戳
        producer.setInstanceName("szz-producer-cliendName");


        producer.start();

       /* DefaultMQProducer producer2 = new DefaultMQProducer(groupName);
        producer2.setInstanceName("szz-producer-cliendName");
        producer2.start();
*/

        Message msg = new Message(topic, tag, "szzkey",
                (" I'm 石臻臻, timestamp:" + System.currentTimeMillis()).getBytes(RemotingHelper.DEFAULT_CHARSET)
        );

        msg.setDelayTimeLevel(5);

        try {
            producer.setSendMsgTimeout(25000);
            SendResult sendResult = producer.send(msg);
            System.out.println(JSONObject.toJSONString(sendResult, SerializerFeature.WriteMapNullValue));
        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(1000);
        }

    }

    public static void sendSyncMsgSelector(String[] args) throws InterruptedException, MQClientException, UnsupportedEncodingException {
        String topic = "szz_select-q1";
        String tag = "Tag-SZZ-msgqueue";
        String groupName = "szz_producer_group_msgqueue";
        // 设置自定义Hook 和 开启消息轨迹
        DefaultMQProducer producer = new DefaultMQProducer(groupName, null, true, null);
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        // 如果设置了 命名空间的话, 最终的ProducerGroup为【${namespace}%groupName】 例如： szz_daily%szz_producer_group
        // 设置生产者客户端实例名称; 可以通过系统属性`rocketmq.client.name` 设置，没有设置的话默认DEFAULT; 但是启动的时候判断如果是DEFAULT，则将它改成：PID@时间戳
        producer.setInstanceName("szz-producer-cliendName");

        //producer.getDefaultMQProducerImpl().registerSendMessageHook(new SzzMessageHook());
        //producer.getDefaultMQProducerImpl().registerCheckForbiddenHook(new SzzForbiddenMessageHook());
        //producer.getDefaultMQProducerImpl().registerEndTransactionHook(new SzzEndTransactionHook());

        producer.setMqClientApiTimeout(3000000);
        // 发送消息超时时间，超过时间不重试的
        producer.setSendMsgTimeout(2500000);
        producer.setSendLatencyFaultEnable(true);
        producer.start();

        Message msg = new Message(topic, tag, "szzkey",
                (" I'm 石臻臻-HOOK, timestamp:" + System.currentTimeMillis()).getBytes(RemotingHelper.DEFAULT_CHARSET)
        );
        //延时消息
        //msg.setDelayTimeLevel(5);

        try {
            // 按照arg参数hash选择队列；但是貌似选择的队列都是目前在线的；所以这个会不准的；
/*
            for (int i = 0; i < 5; i++) {
                SendResult sendResult = producer.send(msg, new SelectMessageQueueByRandom(), "123");
                System.out.println(JSONObject.toJSONString(sendResult, SerializerFeature.WriteMapNullValue));
            }
*/
            //

            //producer.send(msg);

            producer.setEnableBackpressureForAsyncMode(true);
            //producer.setBackPressureForAsyncSendNum(0);







            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {

                    System.out.println("SendCallback#onSuccess...");
                }

                @Override
                public void onException(Throwable e) {

                    System.out.println("SendCallback#onException..."+e.getCause());

                    if(e instanceof RemotingTooMuchRequestException){
                        // 达到限流阈值了/ 或者请求超时了
                        System.out.printf("异步发送发送Topic:%s 失败(超时或者限流了), 异常：%s",msg.getTopic(),e.getMessage());
                    } else if (e instanceof RemotingTimeoutException){
                        System.out.printf("异步发送发送Topic:%s 失败(Netty限流了),异常：%s",msg.getTopic(),e.getMessage());
                    } else if (e instanceof MQClientException){
                        System.out.printf("异步发送发送Topic:%s 失败(客户端异常,部分情况会主动重试),异常：%s",msg.getTopic(),e.getMessage());
                    } else if (e instanceof MQBrokerException){
                        System.out.printf("异步发送发送Topic:%s 失败(Broker处理异常了),异常：%s",msg.getTopic(),e.getMessage());
                    } else if (e instanceof RemotingSendRequestException){
                        System.out.printf("异步发送发送Topic:%s 失败(Netty发起请求异常了),异常：%s",msg.getTopic(),e.getMessage());
                    }

                }
            });

            System.out.println("---------------");

        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(1000);
        }
    }


    public static void sendSyncMsgTestHooks(String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException {

        String topic = "szz_t6";
        String tag = "Tag-SZZ-HOOK";
        String groupName = "szz_producer_group_hook";
        // 设置自定义Hook 和 开启消息轨迹
        DefaultMQProducer producer = new DefaultMQProducer(groupName, new SzzProducerRPCHook(), true, null);
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        // 如果设置了 命名空间的话, 最终的ProducerGroup为【${namespace}%groupName】 例如： szz_daily%szz_producer_group
        // 设置生产者客户端实例名称; 可以通过系统属性`rocketmq.client.name` 设置，没有设置的话默认DEFAULT; 但是启动的时候判断如果是DEFAULT，则将它改成：PID@时间戳
        producer.setInstanceName("szz-producer-cliendName");

        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SzzMessageHook());
        producer.getDefaultMQProducerImpl().registerCheckForbiddenHook(new SzzForbiddenMessageHook());
        producer.getDefaultMQProducerImpl().registerEndTransactionHook(new SzzEndTransactionHook());

        producer.setMqClientApiTimeout(3000000);
        // 发送消息超时时间，超过时间不重试的
        producer.setSendMsgTimeout(2500000);
        producer.setSendLatencyFaultEnable(true);
        producer.setLatencyMax(new long[]{50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L});
        producer.setNotAvailableDuration(new long[]{0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L});
        producer.start();

        Message msg = new Message(topic, tag, "szzkey",
                (" I'm 石臻臻-HOOK, timestamp:" + System.currentTimeMillis()).getBytes(RemotingHelper.DEFAULT_CHARSET)
        );
        //延时消息
        //msg.setDelayTimeLevel(5);

        try {


            SendResult sendResult = producer.send(msg);
            System.out.println(JSONObject.toJSONString(sendResult, SerializerFeature.WriteMapNullValue));
        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(1000);
        }

    }


    public static class SzzProducerRPCHook implements RPCHook {

        @Override
        public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
            // 判断一下发送之前的消息体是不是太大了
            if (request.getBody() != null && request.getBody().length > 10) {
                System.out.println("我是生产者钩子SzzProducerRPCHook, 执行了doBeforeRequest ... ");
            }
        }

        @Override
        public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
            System.out.println("我是生产者钩子SzzProducerRPCHook, 执行了doAfterResponse .... ");

            // 打印一下 Ext属性
            if (MapUtils.isNotEmpty(response.getExtFields())) {
                //response.getExtFields().entrySet().forEach(entry -> System.out.println(entry.getKey() + ":" + entry.getValue()));
            }
        }
    }


    public static class SzzMessageHook implements SendMessageHook {

        @Override
        public String hookName() {
            return "SzzMessageHook";
        }

        @Override
        public void sendMessageBefore(SendMessageContext context) {

            System.out.println("SzzMessageHook#sendMessageBefore.....");
        }

        @Override
        public void sendMessageAfter(SendMessageContext context) {
            System.out.println("SzzMessageHook#sendMessageAfter.....");

        }
    }

    // 消费者
    public static class SzzFilterMessageHook implements FilterMessageHook {


        @Override
        public String hookName() {
            return "SzzFilterMessageHook";
        }

        @Override
        public void filterMessage(FilterMessageContext context) {
            System.out.println("SzzFilterMessageHook#filterMessage.....");

        }

    }

    public static class SzzForbiddenMessageHook implements CheckForbiddenHook {


        @Override
        public String hookName() {
            return "SzzForbiddenMessageHook";
        }

        @Override
        public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
            System.out.println("SzzForbiddenMessageHook#checkForbidden.....");
        }
    }


    public static class SzzEndTransactionHook implements EndTransactionHook {


        @Override
        public String hookName() {
            return "SzzEndTransactionHook";
        }

        @Override
        public void endTransaction(EndTransactionContext context) {
            System.out.println("SzzEndTransactionHook#endTransaction.....");

        }
    }


}