package com.simplekafka.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperClient implements Watcher {

    private static final Logger LOGGER = Logger.getLogger(ZooKeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 30000;

    private final String host;
    private final int port;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZooKeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("connect to zooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("disconnect from zooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("zooKeeper expired, reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public String getConnectString() {
        return host + ":" + port;
    }
}
