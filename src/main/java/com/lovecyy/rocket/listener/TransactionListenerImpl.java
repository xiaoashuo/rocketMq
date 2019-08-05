package com.lovecyy.rocket.listener;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ys
 * @topic
 * @date 2019/8/3 19:45
 * 原生java版
 */
public class TransactionListenerImpl implements TransactionListener {

    private static Map<String, LocalTransactionState> STATE_MAP = new HashMap<>();

    /**
     * 执行具体的业务逻辑
     *
     * @param message 发送的消息对象
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        try {
            System.out.println("用户A账户减500元.");
             Thread.sleep(500); //模拟调用服务
              System.out.println(1/0);
            System.out.println("用户B账户加500元.");
             Thread.sleep(800);
            STATE_MAP.put(message.getTransactionId(),
            LocalTransactionState.COMMIT_MESSAGE);
             // 二次提交确认
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (Exception e) {
            e.printStackTrace();
        }
        STATE_MAP.put(message.getTransactionId(),
                LocalTransactionState.ROLLBACK_MESSAGE);
        // 回滚
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }

    /**
     * 消息回查
     *
     * @param messageExt
     * @return
     */

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        return STATE_MAP.get(messageExt.getTransactionId());
    }
}
