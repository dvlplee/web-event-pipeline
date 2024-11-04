package com.example.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
// 전달받은 Properties 객체로 컨슈머를 생성, 실행하는 스레드.
// poll() 메서드 호출 부분
// ConsumerWorker를 스레드로 실행하면 오바라이드된 run() 메서드가 호출된다
public class ConsumerWorker implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);

    // poll() 메서드를 통해 전달받은 데이터를 임시 저장하는 버퍼.
    // static으로 선언해서 다수의 스레드가 만들어지더라도 동일 변수에 접근. 파티션번호, 메시지값이 들어간다.
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();
    // 오프셋 값과 파일 이름을 저장할 때 오프셋 번호를 붙이는데에 사용
    private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>();

    private final static int FLUSH_RECORD_COUNT = 10; // 버퍼의 크기가 이 값에 도달하면 HDFS에 기록.
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties prop, String topic, int number) {
        logger.info("Generate ConsumerWorker");
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (true) {
                // 1초마다 메시지를 가져옴
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    addHdfsFileBuffer(record); // ConsumerRecord로부터 가져온 데이터를 파티션별로 bufferString에 저장
                }
                // 버퍼에 일정개수가 쌓이고나면 HDFS에 저장.
                // 컨슈머 스레드에 할당된 파티션에 대한 버퍼 데이터만 적재.
                saveBufferToHdfsFile(consumer.assignment());
            }
        } catch (WakeupException e) {
            logger.warn("Wakeup consumer");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    // HDFS 적재 부분
    private void addHdfsFileBuffer(ConsumerRecord<String, String> record) {
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        if (buffer.size() == 1)
            currentFileOffset.put(record.partition(), record.offset());
    }

    private void saveBufferToHdfsFile(Set<TopicPartition> partitions) {
        partitions.forEach(p -> checkFlushCount(p.partition()));
    }

    private void checkFlushCount(int partitionNo) {
        if (bufferString.get(partitionNo) != null) {
            if (bufferString.get(partitionNo).size() > FLUSH_RECORD_COUNT - 1) {
                save(partitionNo);
            }
        }
    }

    private void save(int partitionNo) {
        if (bufferString.get(partitionNo).size() > 0)
            try {
                String fileName = "/data/color-" + partitionNo + "-" + currentFileOffset.get(partitionNo) + ".log";
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem hdfsFileSystem = FileSystem.get(configuration);
                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName));
                fileOutputStream.writeBytes(StringUtils.join(bufferString.get(partitionNo), "\n"));
                fileOutputStream.close();

                bufferString.put(partitionNo, new ArrayList<>());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
    }

    // 컨슈머 스레드 종료 부분
    private void saveRemainBufferToHdfsFile() {
        bufferString.forEach((partitionNo, v) -> this.save(partitionNo));
    }

    public void stopAndWakeup() {
        logger.info("stopAndWakeup");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }
}