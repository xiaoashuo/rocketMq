package com.lovecyy.rocket;

import com.lovecyy.rocket.service.ProducerMessage;
import com.lovecyy.rocket.service.TransactionProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author ys
 * @topic
 * @date 2019/8/4 17:54
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RocketMqSpringBootTest {
    @Autowired
    private ProducerMessage producerMessage;

    @Autowired
    private TransactionProducer transactionProducer;

    @Test
    public void testProduce(){
        producerMessage.send("test-topic-1","今天是个好日子");
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testTransactionProduce(){
        transactionProducer.sendMsg("test-topic-1","今天是个好日子");
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
