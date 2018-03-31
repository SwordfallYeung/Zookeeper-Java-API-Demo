package cn.itcast.zkclock;

import org.apache.zookeeper.*;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @author y15079
 * @create 2018-03-17 18:40
 * @desc 分布式共享锁 单线程的
 *
 * zookeeper的监听是一次性监听
 **/
public class DistributedClientLock {
	//会话超时
	private static final int SESSION_TIMEOUT = 5000;
	//zookeeper集群地址
	private String hosts = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
	private String groupNode = "locks";
	private String subNode = "sub";
	private boolean haveLock = false;

	private ZooKeeper zk;
	//记录client创建的子节点路径
	private volatile String thisPath;

	/**
	 * 连接zk
	 * @throws Exception
	 */
	public void connectZookeeper() throws Exception{
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
			public void process(WatchedEvent watchedEvent) {
				try {
					//判断事件类型，此处只处理子节点变化事件
					if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged && watchedEvent.getPath().equals("/" + groupNode)){
						//获取子节点，并对父节点进行监听, 开始监听，监听锁是否有变化
						//有点像递归，在监听函数里面注册监听，就会不断循环
						List<String> childrenNodes = zk.getChildren("/" + groupNode, true);
						String thisNode = thisPath.substring(("/" + groupNode + "/").length()); //获取父目录下的子节点路径，不带父目录
						//去比较是否自己是最小id， 约定id最小最先拿到锁
						Collections.sort(childrenNodes);
						if (childrenNodes.indexOf(thisNode) == 0){  //indexOf是索引的意思，查看子节点路径在所有子节点中的索引是否为0，为0则证明子节点id最小，最先获取锁
							//访问共享资源处理业务，并且在处理完成之后删除锁
							doSomething();  //删除锁，监听到，暂时不会调用watcher里面的process方法

							//重新注册一把新的锁，不懂为什么注册新的锁
							thisPath = zk.create("/" + groupNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
							//在上面创建好锁后才调用watcher里面的process方法
						}
					}
				} catch (Exception e) {
				}
			}
		});

		//程序一进来就先注册一把锁到zk上，即创建子节点，还没有开始监听，zk watcher里面的process方法不会执行
		thisPath = zk.create("/" + groupNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

		//wait一小会，便于观察
		Thread.sleep(new Random().nextInt(1000));

		//从zk的锁目录下，获取所有子节点，并且监听子节点的变化， 开始监听，监听锁是否有变化， 一次性监听
		List<String> childrenNodes = zk.getChildren("/" + groupNode, true);

		//列表中只有一个子节点，那肯定就是thisPath，说明client获得锁
		if (childrenNodes.size() == 1){
			doSomething(); //删除锁，监听到，暂时不会调用watcher里面的process方法
			//为什么还创建锁？为了下一次client获取锁做好准备
			thisPath = zk.create("/" + groupNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			//在上面创建好锁后才调用watcher里面的process方法
		}
	}

	/**
	 * 共享资源的访问逻辑写在这个方法中
	 * 处理业务逻辑，并释放
	 * @throws Exception
	 */
	private void doSomething() throws Exception{
		try {
			System.out.println("gain lock: " + thisPath);
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}finally {
			System.out.println("finished: " + thisPath );
			//将thisPath删除，监听thisPath的client将获得通知
			//相当于释放锁
			zk.delete(this.thisPath, -1);
		}
	}

	public static void main(String[] args) throws Exception{
		DistributedClientLock dcl = new DistributedClientLock();
		dcl.connectZookeeper();
		Thread.sleep(Long.MAX_VALUE);
	}
}
