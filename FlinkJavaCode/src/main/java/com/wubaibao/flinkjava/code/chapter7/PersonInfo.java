package com.wubaibao.flinkjava.code.chapter7;

/**
 * 通话用户信息
 */
public class PersonInfo {
    public String phoneNum;
    public String name;
    public String city;

    //空参构造
    public PersonInfo() {

    }

    //有参构造
    public PersonInfo(String phoneNum, String name, String city) {
        this.phoneNum = phoneNum;
        this.name = name;
        this.city = city;
    }

    //toString
    @Override
    public String toString() {
        return "PersonInfo{" +
                "phoneNum='" + phoneNum + '\'' +
                ", name='" + name + '\'' +
                ", city='" + city + '\'' +
                '}';
    }

}

