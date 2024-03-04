package org.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.example.interfaces.Queue;
import org.example.utils.DistributedLock;
import org.example.utils.Helper;

import java.util.List;

public class LockQueue implements Queue {
    private ZooKeeper zooKeeper;
    private String queuePath;

    public LockQueue(ZooKeeper zooKeeper, String queuePath) {
        this.zooKeeper = zooKeeper;
        this.queuePath = queuePath;
        Helper.createNodeIdDoesNotExists(zooKeeper, queuePath);
    }


    @Override
    public void enqueue(String item) throws Exception {
        zooKeeper.create(queuePath + "/item", item.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    @Override
    public String dequeue() throws Exception {
        while (true) {
            List<String> queueChildren = zooKeeper.getChildren(queuePath, false);
            if(queueChildren.isEmpty()) return null;

            queueChildren.sort(String::compareTo);

            for(String item: queueChildren) {
                boolean isLockAcquired = false;
                DistributedLock lock = null;
                try {
                    String itemPath = queuePath + "/" + item;

                    lock = new DistributedLock(zooKeeper, queuePath);
                    isLockAcquired = lock.lock();

                    if(isLockAcquired && zooKeeper.exists(itemPath, false) != null) {
                        String dequeuedItem = new String(zooKeeper.getData(itemPath, false, null));
                        zooKeeper.delete(itemPath, -1);
                        return dequeuedItem;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if(lock!=null && isLockAcquired) lock.unlock();
                }
            }
        }
    }
}
