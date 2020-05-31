package com.bfxy.rocketmq.transaction;

import com.bfxy.rocketmq.constants.Const;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.math.RoundingMode;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Project：rocketmq-api     @author 源伟
 * DateTime：2020/5/30 22:37
 * Description：TODO
 */
public class TransactionProduce {

    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("test_produceGroup");
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1024),
                new DefaultThreadFactory()
        );
        producer.setNamesrvAddr("39.108.157.26:9876");
        producer.setExecutorService(executorService);
        producer.setTransactionListener(new TransactionListenerImpl());
        producer.setSendMsgTimeout(16000);
        producer.start();

        Message message = new Message("test_tx_topic", "myTag", ("hello rocketMq").getBytes(RemotingHelper.DEFAULT_CHARSET));
        producer.sendMessageInTransaction(message, "hello");
        Thread.sleep(Integer.MAX_VALUE);
        producer.shutdown();
    }

    /**
     * The default thread factory
     */
    static class DefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "pool-" +
                    poolNumber.getAndIncrement() +
                    "-MQ-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

}
