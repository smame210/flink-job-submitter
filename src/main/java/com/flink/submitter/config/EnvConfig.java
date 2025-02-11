package com.flink.submitter.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author daitf
 * @date 2025/1/16
 */
public class EnvConfig {
    public static String ENV;

    static {
        Properties properties = new Properties();
        try(InputStream inputStream = EnvConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            properties.load(inputStream);
            ENV = properties.getProperty("env");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
