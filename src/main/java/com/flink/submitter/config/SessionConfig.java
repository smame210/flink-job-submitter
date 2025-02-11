package com.flink.submitter.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author daitf
 * @date 2025/1/17
 */
public class SessionConfig {
    public static final String JOB_MANAGER_ADDRESS;

    public static final String FLINK_LIB_JAR_PATH;

    public static final String TASK_JAR_PATH;

    static {
        Properties properties = new Properties();
        try(InputStream inputStream = SessionConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            properties.load(inputStream);
            JOB_MANAGER_ADDRESS = properties.getProperty("job_manager_address");
            FLINK_LIB_JAR_PATH = properties.getProperty("flink_lib_jar_path");
            TASK_JAR_PATH = properties.getProperty("task_jar_path");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
