package org.example;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.UUID;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class DistributedLock {
    private static final String rootNode = "/locks";
    private static ZooKeeper zooKeeper;
    private static Watcher watcher;

    public static void main(String[] args) throws Exception {
        String sessionId = UUID.randomUUID().toString();

        watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    try {
                        getLockOrWait(sessionId, rootNode);
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        zooKeeper = new ZooKeeper("localhost:2181", 20000, watcher);
        if(zooKeeper.exists(rootNode, false) == null) {
            zooKeeper.create(rootNode, new byte[0], OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        zooKeeper.create(rootNode + "/", sessionId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        getLockOrWait(sessionId, rootNode);
    }

    private static void getLockOrWait(String sessionId, String rootNode) throws Exception{
        List<String> children = zooKeeper.getChildren(rootNode, false);

        children.sort(String::compareTo);
        byte[] data = zooKeeper.getData(rootNode + "/" + children.get(0), false, null);
        if(data!=null && new String(data).equals(sessionId)) {
            System.out.println("Lock Acquired");
            Thread.sleep(5000);
            zooKeeper.delete(rootNode + "/" + children.get(0), -1);
            System.out.println("Lock Released");
        } else {
            System.out.println("Unable to acquire lock, watching for changes on the lock's children");
            zooKeeper.getChildren(rootNode, true);
        }
        Thread.sleep(100_100_100);
    }




}
