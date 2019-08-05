package com.lovecyy.rocket.service;


import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

/**
 * @author ys
 * @topic
 * @date 2019/8/4 18:09
 */
@Service
public class TransactionProducer {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;
    /**
     * 发送消息
     *
     * @param topic
     * @param msg
     */
    public void sendMsg(String topic, String msg) {
        Message message = MessageBuilder.withPayload(msg).build();
        // myTransactionGroup要和@RocketMQTransactionListener(txProducerGroup ="myTransactionGroup")定义的一致
        this.rocketMQTemplate.sendMessageInTransaction("myTransactionGroup", topic, message,
                null);
        System.out.println("发送消息成功");
    }
    }
