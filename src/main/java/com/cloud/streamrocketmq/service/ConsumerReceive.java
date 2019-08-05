package com.cloud.streamrocketmq.service;

import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Service;

/**
 * @author ys
 * @topic
 * @date 2019/8/5 12:46
 */
@Service
public class ConsumerReceive {

    @StreamListener("input")
    public void receiveInput(String message){
        System.out.println("接收到消息:"+message);
    }
}
