package testElectLeader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class LeaderElection implements Watcher{
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private ZooKeeper zooKeeper;
	
	private String ELECTION_NAMESPACE  = "/election";
	private String currentZnodeName;
	
	private static final String TARGET_ZNODE = "/target_znode";
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		LeaderElection le = new LeaderElection();
		le.connectToZooKeeper();
		le.volunteerForLeadership();
		le.electLeader();
		le.watchTargetZnode();
		le.run();
		le.close();
	}
	
	public void volunteerForLeadership() throws KeeperException, InterruptedException {
		String znodePrefix = ELECTION_NAMESPACE+"/c_";
//		zooKeeper.create(path, data, acl, createMode)
		String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("znode name "+ znodeFullPath);
		this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE+'/', "");
	}
	
	public void electLeader() throws KeeperException, InterruptedException {
		List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
		Collections.sort(children);
		String smallest = children.get(0);
		if(smallest.equals(this.currentZnodeName)){
			System.out.println("I am the leader");
		}else {
			System.out.println("I am not the leader... the leader is "+smallest);
		}
	}
	
	// connect to zooKeeper server, provide the server address, the session time out, and watcher
	public void connectToZooKeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}
	
	// Because the communication between client to server is asyncronize, we need to keep the main thread
	public void run() throws InterruptedException {
		synchronized(zooKeeper) {
			zooKeeper.wait();
		}
	}
	public void close() throws InterruptedException {
		zooKeeper.close();
		System.out.println("disconnected from zooKeeper");
	}
	
	public void watchTargetZnode() throws KeeperException, InterruptedException {
		Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
		if(stat==null) {
			return;
		}else {
//			zooKeeper.getData(path, watch, stat)
			byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
			List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);
			System.out.println("data: "+new String(data) + " children :"+ children);
		}
	}
	
	// the event thread
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		switch(event.getType()) {
			case None:
				if(event.getState()==Event.KeeperState.SyncConnected) {
					System.out.println("successfully connected to ZooKeeper");
				}else {
					synchronized(zooKeeper) {
						zooKeeper.notifyAll();
					}
				}
				break;
			case NodeDeleted:
				System.out.println("TARGET_ZNODE"+" was deleted");
				break;
			case NodeCreated:
				System.out.println("TARGET_ZNODE"+" was created");
				break;
			case NodeDataChanged:
				System.out.println("TARGET_ZNODE" + "data changed");
				break;
			case NodeChildrenChanged:
				System.out.println("TARGET_ZNODE" + "children changed");
				break;
		}
		try {watchTargetZnode();} catch (KeeperException e) {} catch (InterruptedException e) {}
	}
}
