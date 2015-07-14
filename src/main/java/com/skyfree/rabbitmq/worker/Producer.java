package com.skyfree.rabbitmq.worker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;


/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/14 10:58
 */
public class Producer {
    private final static String QUEUE_NAME = "foo";

    public static void main(String[] args) throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("skyfree1");
        factory.setVirtualHost("skyfree");

        factory.setUsername("skyfree");
        factory.setPassword("19840406");

        /**
         * Connection是对Socket Connection的抽象
         */
        Connection connection = factory.newConnection();

        /**
         * channel是使用RabbitMQ的使用接口
         */
        Channel channel = connection.createChannel();

        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);
        
        String message = getMessage(args);

        channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println("[x] Sent '" + message + "'");

        channel.close();
        connection.close();

    }

    private static String getMessage(String[] args) {
        if (args.length < 1) {
            return "Hello World!";
        }
        return joinStrings(args, " ");
    }

    private static String joinStrings(String[] args, String delimiter) {
        int length = args.length;
        if (length == 0) return "";

        StringBuilder words = new StringBuilder(args[0]);

        for (int i = 1; i < args.length; i++) {
            words.append(args[i]);
        }
        return words.toString();
    }
}
