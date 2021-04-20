package com.atguigu.day03.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class Test {
    public static void main(String[] args) {
        TreeSet list = new TreeSet();
        Student s1 = new Student("aa",30);
        Student s2 = new Student("bb",25);
        Student s3 = new Student("cc",21);
        Student s4 = new Student("cc",32);
        Student s5 = new Student("cc",18);
        Student s6 = new Student("cc",18);
        list.add(s1);
        list.add(s2);
        list.add(s3);
        list.add(s4);
        list.add(s5);
        list.add(s6);
        System.out.println(list);
    }
}
