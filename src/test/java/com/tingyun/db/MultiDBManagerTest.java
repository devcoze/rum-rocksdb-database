package com.tingyun.db;

import org.junit.Before;

import java.io.IOException;

class MultiDBManagerTest {

    @Before
    public void init() throws IOException {
        DBManagerConfig config = new DBManagerConfig();
        config.setDataDir("/data/db");
        config.setMaxDBSize(10);
        config.setAllowCleanOldDB(true);
        config.setMaxOpenDB(100);
        config.setMaxIdleTime(10);
        ReadonlyMultiDBManager<Long, String> longStringReadonlyMultiDBManager = new ReadonlyMultiDBManager<>(config);
    }

}