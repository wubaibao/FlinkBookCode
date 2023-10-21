package com.wubaibao.flinkjava.code.chapter6.kryoser;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StudentSerializer extends Serializer {
    @Override
    public void write(Kryo kryo, Output output, Object o) {
        Student student = (Student) o;
        output.writeInt(student.id);
        output.writeString(student.name);
        output.writeInt(student.age);
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        Student student = new Student();
        student.id = input.readInt();
        student.name = input.readString();
        student.age = input.readInt();
        return student;

    }
}
