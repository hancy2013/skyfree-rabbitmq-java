package com.skyfree.rabbitmq.broadcast;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;


/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/14 10:58
 */
public class Producer {
    private final static String EXCHANGE_NAME = "logs";

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

        /**
         * 定义了一个fanout类型的exchange
         */
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        String message = "hello world!";

        /**
         * 这里只指定了exchange,没有指定route_key
         */
        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
        System.out.println("[x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
