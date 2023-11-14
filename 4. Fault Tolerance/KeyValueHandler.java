import java.io.*;
import java.net.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.curator.framework.api.*;
import com.google.common.util.concurrent.Striped;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;


public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private volatile Boolean prim = false;
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> clients = null;
    private Striped<Lock> lock = Striped.lock(64);
    private int nodeIndex = 32;
    private ReentrantLock reentLock = new ReentrantLock();

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        curClient.sync();
        List<String> nodes = curClient.getChildren().usingWatcher(this).forPath(zkNode);
        if (nodes.size() == 1) {
                this.prim = true;
        } else {
            Collections.sort(nodes);
            String[] hostAndPort = curClient.getData().forPath(zkNode + "/" + nodes.get(nodes.size() - 1)).toString().split(":");
            this.prim = !(hostAndPort.equals(host) && Integer.valueOf(hostAndPort[1]) == port);
        }
        myMap = new ConcurrentHashMap<String, String>();
    }

    synchronized public void process(WatchedEvent event) throws org.apache.thrift.TException {
        try {
            curClient.sync();
            List<String> nodes = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            if (nodes.size() == 1) {
                this.prim = true;
                return;
            }
            Collections.sort(nodes);
            String[] backupHostAndPort = curClient.getData().forPath(zkNode + "/" + nodes.get(nodes.size() - 1)).toString().split(":");
            this.prim = !(backupHostAndPort[0].equals(host) && Integer.valueOf(backupHostAndPort[1]) == port);
            if (this.prim && this.clients == null) {
                KeyValueService.Client newClient = null;
                while (newClient == null) {
                    try {
                        TTransport tTransport = new TFramedTransport(new TSocket(backupHostAndPort[0], Integer.valueOf(backupHostAndPort[1])));
                        tTransport.open();
                        TProtocol tProtocol = new TBinaryProtocol(tTransport);
                        newClient = new KeyValueService.Client(new TBinaryProtocol(tTransport));
                    } catch (Exception e) { }
                }
                reentLock.lock();
                this.myMap = new ConcurrentHashMap<String, String>(this.myMap);
                this.clients = new ConcurrentLinkedQueue<KeyValueService.Client>();
                for(int i = 0; i < nodeIndex; i++) {
                    TTransport tTransport = new TFramedTransport(new TSocket(backupHostAndPort[0], Integer.valueOf(backupHostAndPort[1])));
                    tTransport.open();
                    this.clients.add(new KeyValueService.Client(new TBinaryProtocol(tTransport)));
                }
                reentLock.unlock();
            } else {
                this.clients = null;
            }
        } catch (Exception e) { this.clients = null; }
    }

    public String get(String key) throws org.apache.thrift.TException
    {
        if (!prim) {
            throw new org.apache.thrift.TException("Backup is not allowed to get.");
        }
        try {
            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
        if (!prim) {
            throw new org.apache.thrift.TException("Backup is not allowed to put.");
        }
        Lock stripedLock = lock.get(key);
        stripedLock.lock();
        while (reentLock.isLocked()) { }
        try {
            myMap.put(key, value);
            if (this.clients != null) {
                KeyValueService.Client curClientBackup = null;
                while(curClientBackup == null) {
                    curClientBackup = clients.poll();
                }
                Lock stripedLockBackup = lock.get(key);
                stripedLockBackup.lock();
                try {
                    myMap.put(key, value);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    stripedLockBackup.unlock();
                }
                this.clients.offer(curClientBackup);
            }
        } catch (Exception e) {
            e.printStackTrace();
            this.clients = null;
        } finally {
            stripedLock.unlock();
        }
    }

    public void makePrimary() throws org.apache.thrift.TException {
        this.prim = true;
    }
}
