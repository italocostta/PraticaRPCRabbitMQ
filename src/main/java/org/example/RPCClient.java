package org.example;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class RPCClient implements AutoCloseable {

    private final Connection connection;
    private final Channel channel;
    private final String replyQueueName;

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();

        replyQueueName = channel.queueDeclare().getQueue();
    }

    public String call(String message) throws Exception {
        String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        String requestQueueName = "rpc_queue";
        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        final String[] response = {null};
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response[0] = new String(delivery.getBody(), "UTF-8");
            }
        };

        channel.basicConsume(replyQueueName, true, deliverCallback, consumerTag -> {});

        while (response[0] == null) {
            Thread.sleep(100);
        }

        return response[0];
    }

    public void close() throws Exception {
        connection.close();
    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            String message = "Request from Ítalo Costa Soares de Oliveira";
            System.out.println(" [x] Requesting: " + message);
            String response = fibonacciRpc.call(message);
            System.out.println(" [.] Got '" + response + "'");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
