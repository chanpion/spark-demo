package com.chenpp.spark.spring;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.hive.HiveContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

/**
 * @author April.Chen
 * @date 2023/4/28 3:33 下午
 **/
@SpringBootApplication
public class StartApplication implements CommandLineRunner {
    @Autowired
    private HiveContext hc;

    public static void main(String[] args) {
        SpringApplication.run(StartApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Dataset<Row> df = hc.sql("select count(1) from LCS_DB.STAFF_INFO");
        List<Long> result = df.javaRDD().map((Function<Row, Long>) row -> row.getLong(0)).collect();
        result.forEach(System.out::println);
    }
}