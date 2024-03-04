package org.example.tester;

import org.apache.zookeeper.ZooKeeper;
import org.example.LockQueue;
import org.example.interfaces.Queue;
import org.example.utils.Constants;
import org.example.utils.Helper;

import java.io.IOException;

public class Consumer {
    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(Constants.zookeeperUrl, 20000, null);

        Helper.createNodeIdDoesNotExists(zk, Constants.queuePath);
        Helper.createNodeIdDoesNotExists(zk, Constants.lockRootPrefix);


        Queue queue = new LockQueue(zk, Constants.queuePath);

        while (true){
            String dequeValue = queue.dequeue();
            if(dequeValue != null){
                System.out.println(dequeValue);
            }
            Thread.sleep(1000);
        }
    }
}
