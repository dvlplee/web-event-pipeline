package com.bogolee;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;

// REST API 호출을 받는 ProduceController
@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*") // 모든 도메인의 요청을 허용
public class ProduceController {
    private final Logger logger = LoggerFactory.getLogger(ProduceController.class);

    // 스프링이 제공하는 '카프카 템플릿(클래스)'을 사용하여 데아터를 전송한다.
    private final KafkaTemplate<String, String> kafkaTemplate;

    public ProduceController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    // 컨트롤러 액션
    @GetMapping("/api/select")
    public void selectColor(
            @RequestHeader("user-agent") String userAgentName,
            @RequestParam(value = "color") String colorName,
            @RequestParam(value = "user") String userName) {
        // 날짜와 시간을 변환할 때 사용할 '포맷'을 정의한다.
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
        Date now = new Date();
        Gson gson = new Gson();

        UserEventVo userEventVo = new UserEventVo(sdfDate.format(now), // 현재 시간을 포맷팅하여 문자열로 변환
                userAgentName, colorName, userName);
        String jsonColorLog = gson.toJson(userEventVo); // userEventVo 객체를 JSON형태 문자열로 변환

        kafkaTemplate.send("select-color", jsonColorLog).addCallback // 전송 결과에 대한 콜백을 추가하는 메서드
                (new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info(result.toString()); // 전송 성공시 결과 출력
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error(ex.getMessage(), ex); // 전송 실패시 에러 출력
            }
        });
    }
}
