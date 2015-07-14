package com.skyfree.rabbitmq.rpc;


import com.rabbitmq.client.*;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/14 15:08
 */
public class RPCServer {
    private static String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) throws Exception {
        if (n == 0) return 0;
        if (n == 1) return 1;

        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("skyfree1");
        factory.setVirtualHost("skyfree");

        factory.setUsername("skyfree");
        factory.setPassword("19840406");

        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();

        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

        channel.basicQos(1);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

        System.out.println("[x] Awaiting RPC requests");

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            AMQP.BasicProperties props = delivery.getProperties();

            AMQP.BasicProperties replayProps = new AMQP.BasicProperties().builder().correlationId(props.getCorrelationId()).build();

            String message = new String(delivery.getBody());
            int n = Integer.parseInt(message);
            System.out.println(n);
            System.out.println("[.] fib(" + message + ")");
            String response = "" + fib(n);
            System.out.println(props.getReplyTo());
            channel.basicPublish("", props.getReplyTo(), replayProps, response.getBytes());
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }
}
