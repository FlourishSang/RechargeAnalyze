import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.*;
import java.util.Properties;

/**
 * @BelongsProject: RechargeAnalyze
 * @BelongsPackage: PACKAGE_NAME
 * @Author: Flourish Sang
 * @CreateTime: 2019-02-26 20:06
 * @Description: ${Description}
 */
public class CollectLog {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("metadata.broker.list","hadoop:9092,hadoop02:9092,hadoop03:9092");
        //消息传递到broker时的序列化方式
        properties.setProperty("serializer.class", StringEncoder.class.getName());
        //zk的地址
        properties.setProperty("zookerper.connect","hadoop:2181,hadoop02:2181,hadoop03:2181");
        //是否反馈消息，0是不反馈消息，1是反馈消息
        properties.setProperty("request.required.acks","1");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String,String> producer = new Producer(producerConfig);
        try {
            BufferedReader bf = new BufferedReader(new FileReader(new File("C://data/cmcc.json")));
            String line;
            while ((line = bf.readLine())!=null){
                KeyedMessage<String,String> keyedMessage = new KeyedMessage<>("recharge",line);
                Thread.sleep(2000);
                producer.send(keyedMessage);
                System.out.println(keyedMessage);
            }
            bf.close();
            producer.close();
            System.out.println("发送完毕");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
