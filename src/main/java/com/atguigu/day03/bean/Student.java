package com.atguigu.day03.bean;

public class Student implements Comparable{
    String name;
    int age;

    public Student(){

    }
    public Student(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public int compareTo(Object o) {
        Student stu = (Student) o;
        //if(this.age != stu.age)
        return this.age - stu.age;
        //return -1;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
