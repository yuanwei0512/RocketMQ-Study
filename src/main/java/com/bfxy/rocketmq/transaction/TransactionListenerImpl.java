package com.bfxy.rocketmq.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Project：rocketmq-api     @author 源伟
 * DateTime：2020/5/30 22:54
 * Description：TODO
 */
public class TransactionListenerImpl implements TransactionListener {

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object arg) {
        System.out.println("--------执行本地事务--------");
        String callArg = (String)arg;
        System.out.println("callArg:" + callArg);
        System.out.println("msg:" + message);

        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        System.out.println("--------执行回调事务--------");

        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
