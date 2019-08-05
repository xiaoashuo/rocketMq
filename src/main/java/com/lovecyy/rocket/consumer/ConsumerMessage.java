package com.lovecyy.rocket.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

/**
 * @author ys
 * @topic
 * @date 2019/8/4 17:48
 */
@Service
@RocketMQMessageListener(topic = "test-topic-1", consumerGroup = "my-consumer_test-topic-1"
,selectorExpression = "*")
public class ConsumerMessage implements RocketMQListener<String> {
    @Override
    public void onMessage(String s) {
        System.out.println("接收到消息"+s);
    }
}
