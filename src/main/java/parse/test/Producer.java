package parse.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class Producer {

    public static void main(String[] args) throws FileNotFoundException {
        //Här matar användaren in filens path för att scannas
        String path;
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Please input the syslog.txt path:");

            path = scanner.nextLine();
        }
        //Scannern skannar varje rad
        File file = new File(path);
        Scanner scan = new Scanner(file);
        List<String> lines = new ArrayList<>();
        while (scan.hasNextLine()) {
            lines.add(scan.nextLine());
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Producern skickar den skannade datan till topic "parserTest"
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < lines.size(); i++) {
                System.out.println(lines.get(i));
                kafkaProducer.send(new ProducerRecord<>("parserTest", Integer.toString(i), lines.get(i)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Messages sent successfully.");
        }
    }

}