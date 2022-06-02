package com.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author CoreyChen Zhang
 * @date 2022/5/31 17:11
 * @describe TODO
 */
public class Consumer {

    public static void main(String[] args) throws Exception{
        //设置消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my-group-name-A");
        //设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //NameServer地址， 多个地址用分号(;)分隔
        consumer.setNamesrvAddr("10.255.249.3:9876");
        //参数1：topic名字 参数2：tag名字
        consumer.subscribe("topic-name-A", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动，会一直监听消息
        consumer.start();
        System.out.println("Consumer Started!");
        //consumer.shutdown();
    }
}
