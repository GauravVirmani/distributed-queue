package org.example.tester;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.example.utils.Constants;
import org.example.utils.DistributedLock;

import java.io.IOException;
import java.util.UUID;

public class DistributedLockTester {
    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        ZooKeeper zooKeeper = new ZooKeeper(Constants.zookeeperUrl, 200000, null);
        String uuid = "/" + UUID.randomUUID().toString();
        DistributedLock lock = new DistributedLock(zooKeeper, uuid);
        while (true) {
            if(lock.lock()) {
                System.out.println("Lock ACQUIRED by: " + uuid);
                Thread.sleep(1000);
                lock.unlock();
                System.out.println("Lock RELEASED by: " + uuid);
            } else {
                System.out.println("Unable to acquire lock: " + uuid);
            }
            Thread.sleep(20);
        }
    }
}
