package com.tingyun.db;

import lombok.Data;

/**
 * 数据库管理配置类
 * @author chenlt
 */
@Data
public class MultiDBManagerConfig {

    /**
     * 数据存储目录
     */
    private String dataDir = "/data/db";
    /**
     * 默认最大数据存储量 (GB), 该数据存储目录下所有数据库总和
     */
    private int maxDBSize = 10; // 10GB
    /**
     * 同时打开的数据库个数
     */
    private int maxOpenDB = 100;
    /**
     * 数据库最大空闲时间, 超过该时间未被访问则关闭, 单位: 分钟
     */
    private int maxIdleTime = 30;
    /**
     * 数据库相关配置
     */
    private DatabaseConfig dbConfig;

    /**
     * 清理任务相关配置, 单位: 分钟
     */
    private int cleanTaskDelay = 5;
    /**
     * 清理任务执行周期, 单位: 分钟
     */
    private int cleanTaskPeriod = 30;

    /**
     * 数据库相关配置
     */
    @Data
    public static class DatabaseConfig {

        /**
         * 版本DB数量, 超过该数量未被访问的版本DB将被关闭
         */
        private int versionDbCount = 10;
        /**
         * 版本过期时间, 超过该时间未被访问则关闭, 单位: 分钟
         */
        private int versionExpireTime = 30; // 分钟
        /**
         * 清理过期版本的时间, 单位: 小时
         */
        private int versionClearTime = 24; // 小时

    }


}
