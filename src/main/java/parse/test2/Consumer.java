package parse.test2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Consumer {

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        List<String> topics = new ArrayList<>();
        topics.add("parserTest");

        String path;
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Please input the output path:");

            path = scanner.nextLine();
        }

        kafkaConsumer.subscribe(topics);
        String fileContent = "";

        String fileName = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        FileWriter writer = new FileWriter(/*"C:/arbetsprov/arbetsprov-java-parser/"*/path + fileName + ".json");

        try{
            System.out.println("Waiting for producer...");
            while (true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1L));
                for (ConsumerRecord<String, String> record: records){
                    //Delar upp och konverterar data till hanterbara variabler.
                    String syslog = record.value();
                    String year =new SimpleDateFormat("yyyy").format(new Date());
                    String readMonth = syslog.substring(0,3);
                    DateFormat format = new SimpleDateFormat("MMM");
                    SimpleDateFormat sdf=new SimpleDateFormat("MM");
                    Date parseMonth = format.parse(readMonth);
                    String month = sdf.format(parseMonth);
                    String currentMonth = new SimpleDateFormat("MM").format(new Date());
                    if (month.equals("12") && currentMonth.equals("01")){ //Om loggarna är från December förra året. Detta antar att det inte tar längre än en månad att hantera loggarna.
                        int test = Integer.parseInt(year) - 1;
                        year = Integer.toString(test);
                    }
                    String date = syslog.substring(4,6);
                    String time = syslog.substring(7,15);
                    date = date.replaceAll("\\s+","0");
                    String timestamp = year + "-" + month + "-" + date + "'T'" + time + new SimpleDateFormat(".SSSSSSXXX").format(new Date());
                    String id = syslog.substring(16,20);
                    String process = syslog.substring(21,25);
                    String pid = syslog.substring(27,31);
                    String message = syslog.substring(34);
                    String ip = "localhost";
                    String type;
                    String created = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").format(new Date());

                    //Avgör typ för loggmeddelandet
                    String access  = "connect";
                    String allowed = "accept";
                    String denied  = "reject";
                    String error   = "error";
                    String start   = "opened";
                    String stop    = "closed";
                    if (message.toLowerCase().contains(access.toLowerCase())) {
                        type = "access";
                    } else if (message.toLowerCase().contains(allowed.toLowerCase())) {
                        type = "allowed";
                    } else if (message.toLowerCase().contains(denied.toLowerCase())) {
                        type = "denied";
                    } else if (message.toLowerCase().contains(error.toLowerCase())) {
                        type = "error";
                    } else if (message.toLowerCase().contains(start.toLowerCase())) {
                        type = "start";
                    } else if (message.toLowerCase().contains(stop.toLowerCase())) {
                        type = "stop";
                    }else {
                        type = "unknown";
                    }

                    //Tolkar till JSON.
                    fileContent = fileContent.concat("{" + "\n");
                    fileContent = fileContent.concat("\t\"@timestamp\": " + "\"" + timestamp + "\"" + "\n");
                    fileContent = fileContent.concat("\t\"message\": " + "\"" +  message + "\"" + "\n");
                    fileContent = fileContent.concat("\t\"agent\": {" + "\n");
                    fileContent = fileContent.concat("\t\t\"id\": " + "\"" + id + "\"" + "\n");
                    fileContent = fileContent.concat("\t}," + "\n");
                    fileContent = fileContent.concat("\t\"host\": {" + "\n");
                    fileContent = fileContent.concat("\t\t\"ip\": " + "\"" + ip + "\"" + "\n");
                    fileContent = fileContent.concat("\t}," + "\n");
                    fileContent = fileContent.concat("\t\"event\": {" + "\n");
                    fileContent = fileContent.concat("\t\t\"created\": " + "\"" +  created + "\" ," + "\n");
                    fileContent = fileContent.concat("\t\t\"type\": " + "\"" +  type + "\"" + "\n");
                    fileContent = fileContent.concat("\t}," + "\n");
                    fileContent = fileContent.concat("\t\"process\": {" + "\n");
                    fileContent = fileContent.concat("\t\t\"pid\": " + "\"" + pid + "\"" + "\n");
                    fileContent = fileContent.concat("\t\t\"name\": " + "\"" + process + "\"" + "\n");
                    fileContent = fileContent.concat("\t}" + "\n");
                    fileContent = fileContent.concat("}" + "\n");

                }
                //Skriver fil och avslutar processen om filen inte är tom
                if (!fileContent.equals("")) {
                    writer.write(fileContent);
                    writer.close();
                    System.out.println("Successfully created json file.");
                    break;
                }

            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}
