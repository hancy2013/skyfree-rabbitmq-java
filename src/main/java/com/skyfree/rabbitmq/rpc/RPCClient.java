package com.skyfree.rabbitmq.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/14 15:19
 */
public class RPCClient {
    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;
    private QueueingConsumer consumer;

    public RPCClient() throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("skyfree1");
        factory.setVirtualHost("skyfree");

        factory.setUsername("skyfree");
        factory.setPassword("19840406");

        connection = factory.newConnection();
        channel = connection.createChannel();

        replyQueueName = channel.queueDeclare().getQueue();
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);
    }

    public String call(String message) throws InterruptedException, IOException {
        String response = null;
        String corrID = java.util.UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties()
                .builder()
                .correlationId(corrID)
                .replyTo(replyQueueName)
                .build();
        
        channel.basicPublish("", requestQueueName, props, message.getBytes());

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            if (delivery.getProperties().getCorrelationId().equals(corrID)) {
                response = new String(delivery.getBody());
                break;
            }
        }

        return response;
    }

    public void close() throws IOException {
        connection.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RPCClient client = new RPCClient();
        String ret = client.call("9");
        System.out.println(ret);
        client.close();
    }
}
