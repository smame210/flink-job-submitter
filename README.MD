# 远程提交flink任务

支持flink standalone提交和flink on yarn的application模式提交。

### 提交方式

```text
1. application.properties文件中配置flink集群和jar包路径；
2. 修改JobStarter类中的FlinkJobArgs参数定义和DeployMode定义；
3. 如果为yarn模式，需修改resource下的hadoop和yarn相关配置以及flink-conf.yaml（如需修改日志配置，可修改resource下的log4j.properties文件）；
4. 执行JobStarter类中的main方法。
```


注：  
windows环境下提交flink on yarn任务存在报错情况，需修改flink-yarn中Utils类和YarnClusterDescriptor类的源码。  
参考文章: [windows使用java远程提交flink任务到yarn集群 找不到或无法加载主类org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint](https://blog.csdn.net/weixin_43039757/article/details/143747042)