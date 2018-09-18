package publishsubscribe;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Consumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(Producer.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        // rabbitmq will create a new queue whenever queueDeclare() is called.
        String queue = channel.queueDeclare().getQueue();
        // an exchange can bind to multiple queues
        channel.queueBind(queue, Producer.EXCHANGE_NAME, "");

        System.out.println("[x] Created queue '" + queue + "'");
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

        // consume from the queue bound to the exchange
        channel.basicConsume(queue, false, consumer);
    }
}
