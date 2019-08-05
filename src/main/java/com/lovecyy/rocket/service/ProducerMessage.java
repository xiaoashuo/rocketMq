package com.lovecyy.rocket.service;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author ys
 * @topic
 * @date 2019/8/4 17:41
 */
@Service
public class ProducerMessage {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;


    public void send(String topic,String msg){
        rocketMQTemplate.convertAndSend(topic,msg);
    }
}
