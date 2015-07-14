package com.skyfree.rabbitmq.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/14 11:08
 */
public class Receiver {
    private final static String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("skyfree1");
        factory.setVirtualHost("skyfree");

        factory.setUsername("skyfree");
        factory.setPassword("19840406");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /**
         * 注释了后面两个add操作,表示,我只感兴趣info级别的信息
         */
        List<String> severities = new ArrayList<String>();
        severities.add("info");
        //severities.add("warning");
        //severities.add("error");

        /**
         * 声明一个exchange
         * 声明一个匿名队列
         */
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();

        for (String severity : severities) {

            /**
             * 通过最后一个参数指定了binding_key, 这里的for循环,进行了多次绑定
             */
            channel.queueBind(queueName, EXCHANGE_NAME, severity);
        }

        System.out.println("[*] Waiting for messages, To exit press Ctrl+C");

        /**
         * 从broker来的消息,先缓存到QueueingConsumer中
         */
        QueueingConsumer consumer = new QueueingConsumer(channel);

        boolean autoAck = true;
        channel.basicConsume(queueName, autoAck, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println("[x] Received: '" + message + "'");

        }
    }

}
