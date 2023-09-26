package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.common.MixAll;

/**
 * @author shizhen(十真) 2023/09/11
 */
public class SzzConsumerMessageHook implements ConsumeMessageHook {
    @Override
    public String hookName() {
        return "SzzTestConsuerMessageHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        System.out.println("SzzConsumerMessageHook#consumeMessageBefore...");
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        System.out.println("SzzConsumerMessageHook#consumeMessageAfter...,消费状态为：" + context.getProps().get(MixAll.CONSUME_CONTEXT_TYPE));
    }
}
