package publishsubscribe;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Producer {

    static final String QUEUE_NAME = "WorkQueue";

    static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        try {
            // any message sent to the fanout exchange will be delivered to all the
            // queues bound to that exchange.
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            for (int i = 0; i < 10; i++) {
                StringBuilder workLoad = new StringBuilder();
                for (int j = 0; j < i; j++) {
                    workLoad.append(".");
                }

                String message = "Message " + i + ": " + workLoad.toString();
                // publish to the exchange
                channel.basicPublish(EXCHANGE_NAME, "",
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
