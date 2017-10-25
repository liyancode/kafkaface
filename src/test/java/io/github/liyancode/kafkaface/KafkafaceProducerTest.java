package io.github.liyancode.kafkaface;

import org.junit.Test;

import java.util.Calendar;

/**
 * Created by {Li,Yan} on 10/25/17.
 */
public class KafkafaceProducerTest {
    private final String testTopic="KafkafaceProducerTest_topic";
    @Test
    public void testProduce(){
        KafkafaceProducer kp=new KafkafaceProducer("localhost","9092","","");
        kp.produce(testTopic,"key_01", Calendar.getInstance().toString()+"_testmessage");
        kp.close();
    }
}