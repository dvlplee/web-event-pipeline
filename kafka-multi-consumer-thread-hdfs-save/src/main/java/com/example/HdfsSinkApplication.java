package com.example;

import com.example.consumer.ConsumerWorker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
 
// main 스레드. 컨슈머 설정(configs)를 컨슈머 스레드에 넘김.
public class HdfsSinkApplication {
    private final static Logger logger = LoggerFactory.getLogger(HdfsSinkApplication.class);
    private final static String BOOTSTRAP_SERVERS = "";
    private final static String TOPIC_NAME = "select-color";
    private final static String GROUP_ID = "color-hdfs-save-consumer-group";
    private final static int CONSUMER_COUNT = 3; // 컨슈머 스레드 개수
    private final static List<ConsumerWorker> workers = new ArrayList<>(); // ConsumerWorker 객체를 저장하는 workers 리스트.

    public static void main(String[] args) {
        // Shutdown Hook으로 등록된 스레드는 애플리케이션 종료 시 자동으로 start()가 호출된다.
        // ShutdownThread를 등록하여 애플리케이션이 종료될 때 실행될 스레드를 정의. 강제 종료시 안전하게 컨슈머 종료하기 위함.
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs = new Properties(); // 카프카 컨슈머 설정
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // 멀티스레드를 생성하고 관리하기 위해 ExecutorService 객체 생성.
        // newCachedThreadPool()는 필요한 만큼 스레드 풀을 늘리고, 작업 이후 스레드를 종료한다.
        ExecutorService executorService = Executors.newCachedThreadPool();

        // 컨슈머 개수만큼 ConsumerWorker 객체를 생성하여 workers리스트에 추가.
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            workers.add(new ConsumerWorker(configs, TOPIC_NAME, i));
        }
        // 각 ConsumerWorker를 executorService에서 실행
        // for (ConsumerWorker worker : workers) { executorService.execute(worker); } 와 같다.
        // workers.forEach(worker -> executorService.execute(worker)); 와 같다.
        // 여기서 worker는 workers 리스트의 각 요소인 ConsumerWorker 객체를 뜻하는 임의의 값이다.
        workers.forEach(executorService::execute); // '메서드 참조' 표현. executorService의 execute() 메서드 실행
    }

    // 애플리케이션이 종료될 때 실행할 ShutdownThread 클래스.
    // ShutdownThread 스레드가 시작될 때 run() 메서드가 호출된다.
    // run() 메서드는 스레드에서 실행될 작업을 정의하는 메서드.
    static class ShutdownThread extends Thread {
        @Override
        public void run() {
            logger.info("Shutdown hook");
            workers.forEach(ConsumerWorker::stopAndWakeup); // 각 컨슈머 스레드의 종료를 알리기 위해 stopAndWakup() 메서드 실행.
        }
    }
}
