package com.skyfree.rabbitmq.worker;

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
        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        System.out.println("[*] Waiting for messages, To exit press Ctrl+C");

        /**
         * 从broker来的消息,先缓存到QueueingConsumer中
         */
        QueueingConsumer consumer = new QueueingConsumer(channel);

        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            doWork(message);
            System.out.println("[x] Received: '" + message + "'");

            /**
             * 向rabbitmq返回ack,rabbitmq接收到ack后,删除对应的消息
             */
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }

    private static void doWork(String message) throws InterruptedException {
        for (char ch : message.toCharArray()) {
            if (ch == '.') {
                Thread.sleep(1000);
            }
        }
    }
}
