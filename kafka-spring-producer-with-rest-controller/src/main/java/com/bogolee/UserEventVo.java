package com.bogolee;

// 사용자 이벤트 객체(VO). 데이터를 담음.
public class UserEventVo {
    private String timeStamp; // REST API를 호출받은 시점을 메시지 값에 넣는 역할
    private String userAgent; // 사용자 브라우저 종류
    private String colorName; // 사용자 입력값1
    private String userName; // 사용자 입력값2

    public UserEventVo(String timeStamp, String userAgent, String colorName, String userName) {
        this.timeStamp = timeStamp;
        this.userAgent = userAgent;
        this.colorName = colorName;
        this.userName = userName;
    }
}
