/**
 * Created by f.touopi.touopi on 2/16/2017.
 */

//import util.properties packages
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

//Create java class named â€œSimpleProducerâ€?
public class VoteProducer {
    private final static String VOTE_TOPIC="vote-topic";
     static final String SEPARATOR=";";

    public static void main(String[] args) throws Exception{

        // Check arguments length value
       /** if(args.length == 0){
            System.out.println("Enter topic name");
            return;
        }*/

        //Assign topicName to string variable
        //String topicName = args[0].toString();

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
       // props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
       props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

                //If the request fails, the producer can automatically retry,
                props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
           
            for (long i=0;i<1500;i++) {
                int randomNum = ThreadLocalRandom.current().nextInt(1, 6 + 1);
                final String msg= new StringBuilder().append(randomNum).append(SEPARATOR).append(UUID.randomUUID().toString()).toString();
                producer.send(new ProducerRecord<>(VOTE_TOPIC,
                        UUID.randomUUID().toString(),msg));
                System.out.println(msg);
                if (i % 10 == 0) {
                    Thread.sleep(1000);
                }
            }
            
            System.out.println("Vote sent successfully");
        }
    }
}
