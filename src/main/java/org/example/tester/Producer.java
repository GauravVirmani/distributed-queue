package org.example.tester;

import org.apache.zookeeper.ZooKeeper;
import org.example.LockQueue;
import org.example.interfaces.Queue;
import org.example.utils.Constants;

import java.io.IOException;

public class Producer {
    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(Constants.zookeeperUrl, 200000, null);

        Queue queue = new LockQueue(zooKeeper, Constants.queuePath);

        for(int i=0; i<1000; ++i) {
            System.out.println("Producing "+ i);
            queue.enqueue("ele" + i);
            Thread.sleep(1000);
        }
        zooKeeper.close();
    }
}
