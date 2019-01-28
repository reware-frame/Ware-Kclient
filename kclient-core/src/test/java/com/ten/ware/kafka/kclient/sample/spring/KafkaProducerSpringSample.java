package com.ten.ware.kafka.kclient.sample.spring;

import com.ten.ware.kafka.kclient.core.KafkaProducer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.fastjson.JSON;
import com.ten.ware.kafka.kclient.sample.domain.Dog;

/**
 * Sample for use {@link KafkaProducer} with Spring context.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaProducerSpringSample {
	public static void main(String[] args) throws InterruptedException {
		ApplicationContext ac = new ClassPathXmlApplicationContext(
				"kafka-producer.xml");

		KafkaProducer kafkaProducer = (KafkaProducer) ac.getBean("producer");

		for (int i = 0; i < 10; i++) {
			Dog dog = new Dog();
			dog.setName("Yours " + i);
			dog.setId(i);
			kafkaProducer.send2Topic("test", JSON.toJSONString(dog));

			System.out.format("Sending dog: %d \n", i + 1);

			Thread.sleep(100);
		}
	}
}
