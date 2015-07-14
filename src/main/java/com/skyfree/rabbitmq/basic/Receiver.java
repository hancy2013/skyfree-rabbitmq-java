package com.skyfree.rabbitmq.basic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/14 11:08
 */
public class Receiver {
    private final static String QUEUE_NAME = "foo";

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("skyfree1");
        factory.setVirtualHost("skyfree");

        factory.setUsername("skyfree");
        factory.setPassword("19840406");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /**
         * queueDeclare是幂等操作
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("[*] Waiting for messages, To exit press Ctrl+C");

        /**
         * 从broker来的消息,先缓存到QueueingConsumer中
         */
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println("[x] Received: '" + message + "'");
        }
    }
}
