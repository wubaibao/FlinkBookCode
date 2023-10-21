package com.wubaibao.flinkjava.code.chapter10.example;

/**
 * LoginInfo :用户登录信息
 *  uid:用户ID
 *  userName: 用户名称
 *  logInTime:登录时间
 *  logInState: 登录状态
 */
public class LoginInfo {
    public String uid;
    public String userName;
    public Long loginTime;
    public String loginState;

    public LoginInfo(String uid, String userName, Long loginTime, String lognState) {
        this.uid = uid;
        this.userName = userName;
        this.loginTime = loginTime;
        this.loginState = lognState;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Long getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(Long loginTime) {
        this.loginTime = loginTime;
    }

    public String getLoginState() {
        return loginState;
    }

    public void setLoginState(String loginState) {
        this.loginState = loginState;
    }

    @Override
    public String toString() {
        return "LoginInfo{" +
                "uid='" + uid + '\'' +
                ", userName='" + userName + '\'' +
                ", loginTime=" + loginTime +
                ", loginState='" + loginState + '\'' +
                '}';
    }
}
