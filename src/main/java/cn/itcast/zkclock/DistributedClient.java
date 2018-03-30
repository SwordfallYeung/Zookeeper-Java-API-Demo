package cn.itcast.zkclock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author y15079
 * @create 2018-03-30 14:55
 * @desc 分布式共享锁，多线程模式下
 **/
public class DistributedClient {
	//超时时间
	private static final int SESSION_TIMEOUT = 5000;
	//zookeeper server列表
	private String hosts = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
	private String groupNode = "locks";
	private String subNode = "sub";

	private ZooKeeper zk;
	//当前client创建的子节点
	private String thisPath;
	//当前client等待的子节点
	private String waitPath;

	private CountDownLatch latch = new CountDownLatch(1);

	public void connectZookeeper() throws Exception{
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
			public void process(WatchedEvent watchedEvent) {
				try {
					//连接建立时，打开latch，唤醒wait在该latch上的线程
					if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
						latch.countDown();
					}

					//发生了waitPath的删除事件
					if(watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)){
						doSomething();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		//等待连接建立
		latch.await();

		//创建子节点
		thisPath = zk.create("/" + groupNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

		//wait一小会，让结果更清晰一些
		Thread.sleep(10);

		//注意，没有必要监听"/locks"的子节点的变化情况
		List<String> childrenNode = zk.getChildren("/" + groupNode, false);

		//列表中只有一个子节点，那肯定就是thisPath，说明client获得锁
		if (childrenNode.size() == 1){
			doSomething();
		}else {
			String thisNode = thisPath.substring(("/" + groupNode + "/").length());
			//排序 约定id最小的最先获取到锁
			Collections.sort(childrenNode);
			int index = childrenNode.indexOf(thisNode);
			if (index == -1){
				//never happened
			}else if (index == 0){
				//index == 0, 说明thisNode 在列表中最小，当前client获得锁
				doSomething();
			}else {
				//获得排名比thisPath前1为的节点
				this.waitPath = "/" + groupNode + "/" + childrenNode.get(index - 1);
				//在waitPath上注册监听器，当waitPath被删除时，zookeeper会回调监听器的process方法
				zk.getData(waitPath, true, new Stat());
			}
		}
	}

	private void doSomething() throws Exception{
		try {
			System.out.println("gain lock: " + thisPath);
			Thread.sleep(2000);
			// do something
		} finally {
			System.out.println("finished: " + thisPath);
			// 将thisPath删除, 监听thisPath的client将获得通知
			// 相当于释放锁
			zk.delete(this.thisPath, -1);
		}
	}

	public static void main(String[] args) throws Exception{
		for (int i = 0; i < 10; i++ ){
			new Thread(){
				@Override
				public void run() {
					try {
						DistributedClient dc = new DistributedClient();
						dc.connectZookeeper();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}.start();
		}

		Thread.sleep(Long.MAX_VALUE);
	}
}
