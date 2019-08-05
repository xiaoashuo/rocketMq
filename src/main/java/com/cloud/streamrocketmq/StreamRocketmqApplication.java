package com.cloud.streamrocketmq;

import com.cloud.streamrocketmq.service.StreamProviderService;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

/**
 * 官方示例
 * https://github.com/alibaba/spring-cloud-alibaba/blob/master/spring-cloud-alibaba-examples/rocketmq-example/readme-zh.md
 */
@SpringBootApplication
@EnableBinding({Source.class, Sink.class})
public class StreamRocketmqApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(StreamRocketmqApplication.class, args);
    }
    @Autowired
    private StreamProviderService output;

    @Override
    public void run(String... args) throws Exception {

        output.send("终于万和城了");
    }

}
