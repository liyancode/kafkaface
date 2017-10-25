package io.github.liyancode.kafkaface;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;

/**
 * Created by {Li,Yan} on 10/25/17.
 */
public class KafkafaceConsumerTest {
    private static Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(KafkafaceConsumerTest.class.getName());
    @Test
    @Ignore
    public void testInit(){
        KafkafaceProducer kp=new KafkafaceProducer("localhost","9092","","");
        kp.produce("KafkafaceProducerTest_topic","key_01", Calendar.getInstance().toString()+"_testmessage");
        kp.close();

        KafkafaceConsumer consumer=new KafkafaceConsumer("localhost","9092","Gtesj","","");
        ArrayList topics=new ArrayList();
        topics.add("KafkafaceProducerTest_topic");
        consumer.subscribe(topics);
        Consumer cnsum=consumer.getConsumer();
        ConsumerRecords<String,String> recds=cnsum.poll(100);
        LOGGER.info(recds.isEmpty());
        for (ConsumerRecord<String, String> record : recds){
            LOGGER.info("k={},v={}",record.key(),record.value());
        }
    }
}