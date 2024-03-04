# Distributed Queue

Distributed queue implemented using zookeeper.

### Example run 
Sample run with 1 producer(top half) and 2 consumers(bottom half) on the queue reading mutually exclusive items

![img.png](src/main/resources/img.png)

### State in zookeeper

Queue maintained in zookeeper as Persistent Sequential nodes to maintain FIFO order as children of `/queue` node.

![img.png](src/main/resources/2.png)