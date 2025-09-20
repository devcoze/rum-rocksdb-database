package com.tingyun.db;

import lombok.Data;

/**
 * 数据库管理配置类
 * @author chenlt
 */
@Data
public class DBManagerConfig {

    /**
     * 数据存储目录
     */
    private String dataDir = "/data/db";
    /**
     * 默认最大数据存储量 (GB), 该数据存储目录下所有数据库总和
     */
    private long maxDBSize = 10; // 10GB
    /**
     * 是否允许清理旧数据库, 当总数据量超过maxDBSize时, 会删除最久未被访问的数据库
     */
    private boolean allowCleanOldDB = true;
    /**
     * 同时打开的数据库个数
     */
    private int maxOpenDB = 100;
    /**
     * 数据库最大空闲时间, 超过该时间未被访问则关闭, 单位: 分钟
     */
    private int maxIdleTime = 10;

    private DatabaseConfig dbConfig;
    /**
     * 数据库相关配置
     */
    @Data
    public static class DatabaseConfig {

    }


}
