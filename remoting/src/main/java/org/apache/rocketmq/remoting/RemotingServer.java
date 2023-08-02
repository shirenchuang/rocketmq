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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface RemotingServer extends RemotingService {

    /**
     * 给指定的请求 注册对应的NettyRequestProcessor处理器; 并且可以设置单独的ExecutorService线程执行器
     * @param requestCode 请求code
     * @param processor 请求处理器
     * @param executor 线程执行器
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    /**
     * 注册默认的请求处理器和线程执行器；(没有对具体Request设置的话就是用这个默认的)
     * @param processor 请求处理器
     * @param executor 线程执行器
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 服务端Server开启的监听端口，默认是10911；可以通过-c 的配置文件 listenPort重新设置；
     * @return
     */
    int localListenPort();

    /**
     * 通过 请求code 获取 事件处理器 和 线程执行器; （如果通过registerProcessor注册过才会有）
     * @param requestCode
     * @return
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * 获取默认的处理器和线程执行器
     * @return
     */
    Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair();

    /**
     * 创建新的RemotingServer ； 这个主要是给 Broker-Container用的；
     * @param port
     * @return
     */
    RemotingServer newRemotingServer(int port);

    /**
     * 异常RemotingServer 这个主要是给 Broker-Container用的；
     * @param port
     */
    void removeRemotingServer(int port);

    /**
     * 同步调用，同步等待请求的结果，直到超时
     */
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
        RemotingTimeoutException;

    /**
     * 异步调用，注册一个回调，请求完成后执行回调
     */
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 直接发送一个请求而不关心响应，没有回调
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException;

}
