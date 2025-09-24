package com.tingyun.db;

import lombok.Data;

/**
 * 数据库管理配置类
 * @author chenlt
 */
@Data
public class MultiDBManagerConfig {

    // 默认的版本DB数量，超过该数量未被访问的版本DB将被关闭
    public static final int DEFAULT_VERSION_DB_COUNT = 10;
    // 默认的版本过期时间，超过该时间未被访问则关闭，单位分钟
    public static final int DEFAULT_VERSION_EXPIRE_TIME = 30; // 分钟
    // 默认的清理过期版本的时间，单位分钟，默认24小时
    public static final int DEFAULT_VERSION_CLEAR_TIME = 24 * 60;

    // ---------------- MultiDBManager配置 ----------------
    /**
     * 数据存储目录
     */
    private String dataDir = "/data/db";
    /**
     * 默认最大数据存储量 (GB), 该数据存储目录下所有数据库总和
     */
    private int maxDiskUsageGB = 20; // 10GB
    /**
     * 同时打开的数据库个数
     */
    private int maxOpenDB = 300;
    /**
     * 数据库最大空闲时间, 超过该时间未被访问则关闭(注意不是清理，只是关闭而已), 单位: 分钟
     */
    private int maxIdleTime = 60;

    // ---------------- 清理任务相关配置 ----------------
    /**
     * 清理任务，延迟启动时间, 单位: 分钟
     */
    private int cleanTaskDelay = 10;
    /**
     * 清理任务执行周期, 单位: 分钟
     */
    private int cleanTaskPeriod = 30;


    // ---------------- 数据库(DataWrapper)相关配置 ----------------
    /**
     * 版本DB数量, 超过该数量未被访问的版本DB将被关闭
     */
    private int dbVersionCount = 10;
    /**
     * 版本过期时间, 超过该时间未被访问则关闭, 单位: 分钟
     */
    private int dbVersionExpire = 30;
    /**
     * 清理过期版本的时间, 单位: 分钟, 必须大于dbVersionExpire, 默认为dbVersionExpire的5倍
     */
    private int dbVersionCleanTime;

}
