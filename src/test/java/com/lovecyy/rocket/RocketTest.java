package com.lovecyy.rocket;

import com.lovecyy.rocket.listener.TransactionListenerImpl;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.security.RunAs;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.List;

/**
 * @author ys
 * @topic
 * @date 2019/8/3 16:27
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = RocketApplication.class)
public class RocketTest {

    /**
     * 测试同步生产消息
     * @throws Exception
     */
    @Test
    public void testSyncProducer() throws  Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("test-group");
        // Specify name server addresses.
        producer.setNamesrvAddr("192.168.136.145:9876");
        //Launch the instance.
         producer.start();
        for (int i = 0; i < 100; i++) {
           //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("TopicTest11" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
          //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n"
                    , sendResult);
        }
       //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }

    /**
     * 异步生产消息
     *   //注意： producer.shutdown()要注释掉，否则发送失败。
     *    原因是，异步发送，还未来得及发送就被关闭了。
     */
    @Test
    public void AsyncProducer() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("LOVE_IM");
                producer.setNamesrvAddr("192.168.136.145:9876;192.168.136.145:9877");
       // 发送失败的重试次数
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.start();
        String msgStr = "用户A发送消息给用户B";
        Message msg = new Message("love_im_topic",
                "SEND_MSG",
                msgStr.getBytes(RemotingHelper.DEFAULT_CHARSET));
        // 异步发送消息
        producer.send(msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("消息状态：" + sendResult.getSendStatus());
                System.out.println("消息id：" + sendResult.getMsgId());
                System.out.println("消息queue：" + sendResult.getMessageQueue());
                System.out.println("消息offset：" + sendResult.getQueueOffset());
            }
            @Override
            public void onException(Throwable e) {
                System.out.println("发送失败！" + e);
            }
        });
        System.out.println("发送成功!");
        Thread.sleep(50000);
        // producer.shutdown();
    }

    //消费消息
    @Test
    public void consumuer() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LOVE_IM");
                consumer.setNamesrvAddr("192.168.136.145:9876;192.168.136.145:9877");

        //完整匹配
        //  consumer.subscribe("haoke_im_topic","SEND_MSG");
        //或匹配
         // consumer.subscribe("haoke_im_topic", "SEND_MSG || SEND_MSG1");
        // 订阅topic，接收此Topic下的所有消息
          consumer.subscribe("love_im_topic",
                "*");
          consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        System.out.println(new String(msg.getBody(),
                                "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("收到消息->" + msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
             /*   if(msgs.get(0).getReconsumeTimes() >= 3){
                        // 重试3次后，不再进行重试
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;*/

            }
        });
        consumer.start();
        Thread.sleep(10000000);
    }

    /**
     * 消息过滤器
     * RocketMQ支持根据用户自定义属性进行过滤，
     * 过滤表达式类似于SQL的where，如：a> 5 AND b ='abc'
     * Exception in thread "main" org.apache.rocketmq.client.exception.MQClientException:
     * CODE: 1 DESC: The broker does not support consumer to filter message by SQL92
     * For more information, please visit the url, http://rocketmq.apache.org/docs/faq/
     * 原因是默认配置下，不支持自定义属性，需要设置开启：
     * #加入到broker的配置文件中
     * enablePropertyFilter=true
     */
    @Test
    public void SyncProducerFilter() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("LOVE_IM");
                producer.setNamesrvAddr("192.168.136.145:9876");
        producer.start();
        String msgStr = "美女001";
        Message msg = new Message("haoke_meinv_topic",
                "SEND_MSG"
                ,
                msgStr.getBytes(RemotingHelper.DEFAULT_CHARSET));
        msg.putUserProperty("age", "18");
        msg.putUserProperty("sex", "女");
        // 发送消息
        SendResult sendResult = producer.send(msg);
        System.out.println("消息状态：" + sendResult.getSendStatus());
        System.out.println("消息id：" + sendResult.getMsgId());
        System.out.println("消息queue：" + sendResult.getMessageQueue());
        System.out.println("消息offset：" + sendResult.getQueueOffset());
        System.out.println(sendResult);
        producer.shutdown();

    }

    /**
     * 消费过滤消息
     */
    @Test
    public void ConsumerFilterDemo() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LOVE_IM");
                consumer.setNamesrvAddr("192.168.136.145:9876");
// 订阅topic，接收此Topic下的所有消息
        consumer.subscribe("haoke_meinv_topic", MessageSelector.bySql("age>=18 AND sex= '女'"));
        consumer.registerMessageListener(new MessageListenerConcurrently() {
                            @Override
                            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                            ConsumeConcurrentlyContext context) {
                                for (MessageExt msg : msgs) {
                                    try {
                                        System.out.println(new String(msg.getBody(),
                                                "UTF-8"));
                                    } catch (UnsupportedEncodingException e) {
                                        e.printStackTrace();
                                    }
                                }
                                System.out.println("收到消息->" + msgs);
                                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                            }
                        });
        consumer.start();
        Thread.sleep(10000);

    }

    @Test
    public void OrderProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("LOVE_ORDER_PRODUCER");
                producer.setNamesrvAddr("192.168.136.145:9876");
        producer.start();
        for (int i = 0; i < 100; i++) {
            String msgStr = "order --> " + i;
            int orderId = i % 10; // 模拟生成订单id
            Message message = new Message("love_order_topic", "ORDER_MSG",
                    msgStr.getBytes(RemotingHelper.DEFAULT_CHARSET));
            //MessageQueueSelector lomba表达式做的匿名内部类
            //第三个参数orderId 对应lomba表达式的arg
            SendResult sendResult = producer.send(message, (mqs, msg, arg) -> {
                //选择消息队列
                Integer id = (Integer) arg;
                int index = id % mqs.size();
                return mqs.get(index);
            }, orderId);
            System.out.println(sendResult);
        }
        producer.shutdown();

    }

    /**
     * 顺序消费
     * @throws Exception
     */
    @Test
    public void OrderConsumer() throws Exception{
        DefaultMQPushConsumer consumer = new
                DefaultMQPushConsumer("LOVE_ORDER_CONSUMER");
                consumer.setNamesrvAddr("192.168.136.145:9876");
        consumer.subscribe("love_order_topic",
                "*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                                                       ConsumeOrderlyContext context) {
                System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
    }


    /**
     * 事务生产者
     * @throws Exception
     */
    @Test
    public void TransactionProducer() throws Exception{
        TransactionMQProducer producer = new
                TransactionMQProducer("transaction_producer");
                producer.setNamesrvAddr("192.168.136.145:9876");
        producer.setRetryTimesWhenSendFailed(3);
       // 设置事务监听器
        producer.setTransactionListener(new TransactionListenerImpl());
        producer.start();
        // 发送消息
        Message message = new Message("pay_topic",
                "用户A给用户B转账500元".getBytes("UTF-8"));
        producer.sendMessageInTransaction(message, null);
        Thread.sleep(999999);
        producer.shutdown();

    }

    /**
     * 事务消费者
     */
    @Test
    public void TransactionConsumer() throws Exception{
        DefaultMQPushConsumer consumer = new
                DefaultMQPushConsumer("transaction_producer");
                consumer.setNamesrvAddr("192.168.136.145:9876");
         // 订阅topic，接收此Topic下的所有消息
        consumer.subscribe("pay_topic", "*");
        // 集群模式 默认集群模式
        //consumer.setMessageModel(MessageModel.CLUSTERING);
        // 广播模式
        //consumer.setMessageModel(MessageModel.BROADCASTING);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        System.out.println(new String(msg.getBody(),
                                "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
            consumer.start();
            Thread.sleep(99999);
     }

     @Test
     public void tes1t(){
        double m=10000.051;
        double n=10000.01;
         System.out.println(m-n);
         BigDecimal bigDecimal = new BigDecimal(Double.toString(m));
         BigDecimal bigDecimal1 = new BigDecimal(Double.toString(n));
         BigDecimal subtract = bigDecimal.subtract(bigDecimal1);
         System.out.println(subtract);
     }
}
