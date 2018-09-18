package workqueue;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Consumer {

    /*
    To see that RabbitMq distributes message in a round-robin fashion, run multiple instances of this Consumer.
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(Producer.QUEUE_NAME, true, false, false, null);

        System.out.println("[*] Waiting for messages...");

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                doWork(message);

                // explicit ACK
                channel.basicAck(envelope.getDeliveryTag(), false);
            }

            private void doWork(String message) {
                System.out.println("[x] Processing message '" + message + "'");

                String working = "Working";
                for (char ch : message.toCharArray()) {
                    if (ch == '.') {
                        working += ".";
                        System.out.println(working);
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                }

                System.out.println("[x] Done processing message '" + message + "'");
            }
        };

        channel.basicQos(1);

        // the consumer will explicitly ack the queue when it finishes processing the message.
        channel.basicConsume(Producer.QUEUE_NAME, false, consumer);
    }
}
