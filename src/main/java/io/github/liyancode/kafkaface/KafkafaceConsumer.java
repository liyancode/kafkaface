package io.github.liyancode.kafkaface;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by {Li,Yan} on 10/24/17.
 */
public class KafkafaceConsumer {
    private static Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(KafkafaceConsumer.class.getName());

    private Consumer consumer;
    private final String keySerializerClassDefault="org.apache.kafka.common.serialization.StringDeserializer";
    private final String valueSerializerClassDefault="org.apache.kafka.common.serialization.StringDeserializer";
    private final String enableAutoCommit="true";
    private final String autoCommitIntervalMs="1000";

    /**
     * @param serverName not null
     * @param serverPort not null
     * @param consumerGroupId not null
     * @param keySerializerClass default 'org.apache.kafka.common.serialization.StringDeserializer'
     * @param valueSerializerClass default 'org.apache.kafka.common.serialization.StringDeserializer'
     */
    public KafkafaceConsumer(String serverName, String serverPort,String consumerGroupId,String keySerializerClass,String valueSerializerClass){
        if(serverName==null||serverName.equals("")){
            LOGGER.error("KafkafaceConsumer init error: serverName must not be null or empty!");
            return;
        }
        if(serverPort==null||serverPort.equals("")){
            LOGGER.error("KafkafaceConsumer init error: serverPort must not be null or empty!");
            return;
        }
        if(consumerGroupId==null||consumerGroupId.equals("")){
            LOGGER.error("KafkafaceConsumer init error: consumerGroupId must not be null or empty!");
            return;
        }
        if(keySerializerClass==null||keySerializerClass.equals("")){
            LOGGER.warn("KafkafaceConsumer init warning: keySerializerClass is null or empty, will use default value '{}'",keySerializerClassDefault);
            keySerializerClass=keySerializerClassDefault;
        }
        if(valueSerializerClass==null||valueSerializerClass.equals("")){
            LOGGER.warn("KafkafaceConsumer init warning: valueSerializerClass is null or empty, will use default value '{}'",valueSerializerClassDefault);
            valueSerializerClass=valueSerializerClassDefault;
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", serverName + ":" + serverPort);
        props.put("group.id", consumerGroupId);
        props.put("enable.auto.commit", enableAutoCommit);
        props.put("auto.commit.interval.ms", autoCommitIntervalMs);
        props.put("key.deserializer", keySerializerClass);
        props.put("value.deserializer", valueSerializerClass);
        try{
            this.consumer=new KafkaConsumer(props);
            LOGGER.info("MyKafkaConsumer init with server:port={}:{},consumerGroupId={},key.deserializer={},value.deserializer={},enableAutoCommit={},autoCommitIntervalMs={}",
                    serverName,serverPort,consumerGroupId,keySerializerClass,valueSerializerClass,enableAutoCommit,autoCommitIntervalMs);
        }catch(Exception e){
            LOGGER.error("KafkafaceConsumer construct error:",e);
        }
    }

    public void subscribe(ArrayList topics){
        try{
            this.consumer.subscribe(topics);
            LOGGER.info("subscribed topics={}",topics);
        }catch(Exception e){
            LOGGER.error("subscribe() error:",e);
        }
    }

    public Consumer getConsumer(){
        return this.consumer;
    }
}
