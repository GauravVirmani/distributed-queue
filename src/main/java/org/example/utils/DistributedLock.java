package org.example.utils;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.UUID;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class DistributedLock {
    private final ZooKeeper zooKeeper;

    private final String lockNodeRoot;
    private String lockNode;
    private static Watcher watcher;

    public DistributedLock(ZooKeeper zooKeeper, String path) {
        this.zooKeeper = zooKeeper;
        this.lockNodeRoot = Constants.lockRootPrefix + path;
        Helper.createNodeIdDoesNotExists(zooKeeper, Constants.lockRootPrefix);
        Helper.createNodeIdDoesNotExists(zooKeeper, lockNodeRoot);
        Helper.createNodeIdDoesNotExists(zooKeeper, path);
    }

    public boolean lock() throws InterruptedException, KeeperException {
        lockNode = zooKeeper.create(lockNodeRoot + "/lock-", new byte[0], OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        List<String> children = zooKeeper.getChildren(lockNodeRoot, false);
        children.sort(String::compareTo);

        int indexOfLockNode = children.indexOf(lockNode.substring(lockNode.lastIndexOf("/") + 1));
        if (indexOfLockNode == 0) {
            return true;
        } else {
            zooKeeper.delete(lockNode, -1);
            return false;
        }
    }

    public void unlock() throws InterruptedException, KeeperException {
        zooKeeper.delete(lockNode, -1);
    }
}
