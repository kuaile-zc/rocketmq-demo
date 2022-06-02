package com.rocketmq.demo;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author CoreyChen Zhang
 * @date 2022/5/31 16:55
 * @describe TODO
 */
public class Provider {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        //设置生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("groupB", getAclRPCHook());
        //指定nameServer的地址, 多个地址用分号分隔
        producer.setNamesrvAddr("10.255.249.3:9876");
        //启动实例
        producer.start();
        Message message = new Message("topicB", "tag", "Message : qqqqqqqqqqqqqqqqqq blog address".getBytes());
        producer.send(message);
        System.out.println("Message sent");
        //关闭生产者，释放资源
        producer.shutdown();
    }

    private static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials("RocketMQ","12345678"));
    }

}
