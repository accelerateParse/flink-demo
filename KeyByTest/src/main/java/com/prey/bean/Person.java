package com.prey.bean;

import lombok.Data;

/**
 * @Author YaoJiangXiao
 * @Date 2022-11-16 10:33
 * @description:
 **/
@Data
public class Person {
    private Integer age;
    private String name;

    public Person() {
    }

    public Person(Integer age, String name) {
        this.age = age;
        this.name = name;
    }
}
