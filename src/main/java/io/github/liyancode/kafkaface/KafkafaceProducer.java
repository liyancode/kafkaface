package io.github.liyancode.kafkaface;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.Logger;

import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 * Created by {Li,Yan} on 10/24/17.
 */
public class KafkafaceProducer{
    static Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(KafkafaceProducer.class.getName());

    private Producer<String, String> producer;
    private final String keySerializerClassDefault="org.apache.kafka.common.serialization.StringSerializer";
    private final String valueSerializerClassDefault="org.apache.kafka.common.serialization.StringSerializer";
    private Date startAt;

    /**
     * @param keySerializerClass if null or empty will set to default "org.apache.kafka.common.serialization.StringSerializer"
     * @param valueSerializerClass if null or empty will set to default "org.apache.kafka.common.serialization.StringSerializer"
     */
    public KafkafaceProducer(String serverName, String serverPort,String keySerializerClass,String valueSerializerClass){
        if(serverName==null||serverName.equals("")){
            LOGGER.error("KafkafaceProducer init error: serverName must not be null or empty!");
            return;
        }
        if(serverPort==null||serverPort.equals("")){
            LOGGER.error("KafkafaceProducer init error: serverPort must not be null or empty!");
            return;
        }
        if(keySerializerClass==null||keySerializerClass.equals("")){
            LOGGER.warn("KafkafaceProducer init warning: keySerializerClass is null or empty, will use default value '{}'",keySerializerClassDefault);
            keySerializerClass=keySerializerClassDefault;
        }
        if(valueSerializerClass==null||valueSerializerClass.equals("")){
            LOGGER.warn("KafkafaceProducer init warning: valueSerializerClass is null or empty, will use default value '{}'",valueSerializerClassDefault);
            valueSerializerClass=valueSerializerClassDefault;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", serverName+":"+serverPort);
        props.put("key.serializer", keySerializerClass);
        props.put("value.serializer", valueSerializerClass);
        try{
            this.producer = new KafkaProducer<String, String>(props);
            LOGGER.info("KafkafaceProducer init with server:port={}:{},key.serializer={},value.serializer={}",
                    serverName,serverPort,keySerializerClass,valueSerializerClass);
            this.startAt= Calendar.getInstance().getTime();
        }catch(Exception e){
            LOGGER.error(e);
            return;
        }

    }

    /**
     * @param topic default String type
     * @param key default String type
     * @param msg default String type
     */
    public void produce(String topic,String key,String msg){
        LOGGER.info("KafkafaceProducer produce start:topic={}",topic);
        Date startTime = Calendar.getInstance().getTime();
        producer.send(new ProducerRecord<String, String>(topic, key, msg));
        LOGGER.info("KafkafaceProducer produce end:topic={}, duration={}ms",topic,Calendar.getInstance().getTime().getTime()-startTime.getTime());
    }

    /**
     * close
     */
    public void close(){
        this.producer.close();
        LOGGER.info("KafkafaceProducer closed, total duration={}ms",Calendar.getInstance().getTime().getTime()-startAt.getTime());
    }
}
