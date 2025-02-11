package com.flink.submitter.bean;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author daitf
 * @date 2025/1/16
 */
public class CustomOptions {

    public static final ConfigOption<String> ENV_JAVA_OPTS = ConfigOptions
            .key("env.java.opts")
            .stringType()
            .defaultValue("")
            .withDescription("A custom java options for your Flink job.");
}
