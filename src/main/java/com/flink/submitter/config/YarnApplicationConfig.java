package com.flink.submitter.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author daitf
 * @date 2025/1/15
 */
public class YarnApplicationConfig {
    public static final String TASK_JAR_PATH;

    public static final String FLINK_LIB_JAR_PATH;

    public static final String FLINK_DIST_JAR_PATH;

    static {
        System.setProperty("HADOOP_USER_NAME", "root");

        Properties properties = new Properties();
        try(InputStream inputStream = YarnApplicationConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            properties.load(inputStream);
            TASK_JAR_PATH = properties.getProperty("task_jar_path");
            FLINK_LIB_JAR_PATH = properties.getProperty("flink_lib_jar_path");
            FLINK_DIST_JAR_PATH = properties.getProperty("flink_dist_jar_path");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
