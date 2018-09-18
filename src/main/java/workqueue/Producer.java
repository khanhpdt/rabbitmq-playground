package workqueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Producer {

    static final String QUEUE_NAME = "WorkQueue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        try {
            // durable queue
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            for (int i = 0; i < 10; i++) {
                StringBuilder workLoad = new StringBuilder();
                for (int j = 0; j < i; j++) {
                    workLoad.append(".");
                }

                String message = "Message " + i + ": " + workLoad.toString();
                // persistent message
                channel.basicPublish("", QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                System.out.println("[x] Sent '" + message + "'");

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
            }

        } finally {
            channel.close();
            connection.close();
        }
    }

}
