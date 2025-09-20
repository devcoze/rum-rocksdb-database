package com.tingyun.db;

public abstract class MultiDBManager {

    protected final String dataDir;

    public MultiDBManager(String dataDir) {
        this.dataDir = dataDir;
    }

}
