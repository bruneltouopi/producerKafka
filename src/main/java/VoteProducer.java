/**
 * Created by f.touopi.touopi on 2/16/2017.
 */

//import util.properties packages

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

//import simple producer packages
//import KafkaProducer packages
//import ProducerRecord packages

//Create java class named “SimpleProducer�?
public class VoteProducer {
    private final static String VOTE_TOPIC = "vote-topic";
    static final String SEPARATOR = ";";

    public static void main(String[] args) throws Exception {
        // Check arguments length value
        /** if(args.length == 0){
         System.out.println("Enter topic name");
         return;
         }*/

        //Assign topicName to string variable
        //String topicName = args[0].toString();

        // create instance for properties to access producer configs
        Properties properties = new Properties();

        //Assign localhost id
        // properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        properties.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        properties.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        properties.put("retries", 0);

        //Specify buffer size in config
        properties.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        properties.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        properties.put("buffer.memory", 33554432);

        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            for (long i = 0; i < 15000000; i++) {
                int randomNum = ThreadLocalRandom.current().nextInt(1, 6 + 1);
                final String msg = new StringBuilder().append(randomNum).append(SEPARATOR).append(UUID.randomUUID().toString()).toString();
                producer.send(new ProducerRecord<>(VOTE_TOPIC,
                        UUID.randomUUID().toString(), msg));
                System.out.println(msg);
                if (i % 10 == 0) {
                    Thread.sleep(1000);
                }
            }

            System.out.println("Vote sent successfully");
        }
    }
}
