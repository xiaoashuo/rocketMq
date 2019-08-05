package com.cloud.streamrocketmq.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

/**
 * @author ys
 * @topic
 * @date 2019/8/4 20:50
 *
 */
@Service
public class StreamProviderService {

   @Autowired
    private MessageChannel output;

    public void send(String message){
         output.send(MessageBuilder.withPayload(message).build());
    }
}
