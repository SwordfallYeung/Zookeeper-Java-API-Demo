package cn.itcast.zkdist;

import org.apache.zookeeper.*;

/**
 * @author y15079
 * @create 2018-03-17 1:12
 * @desc 分布式HA应用  服务端
 **/
public class DistributedServer {

	private static final String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
	private static final int sessionTimeout = 2000;
	private static final String parentNode = "/servers";

	private ZooKeeper zk = null;

	/**
	 * 创建到zk的客户端连接
	 * @throws Exception
	 */
	public void getConnect() throws Exception{
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			public void process(WatchedEvent event) {
				//收到事件通知后的回调函数（应该是我们自己的事件处理逻辑）
				System.out.println(event.getType() + "---" + event.getPath());
				try {
					//挂掉了一台重新注册监听，实现循环监听
					zk.getChildren("/",true);   //有点像递归，在监听函数里面注册监听，就会不断循环。
				} catch (Exception e) {
				}
			}
		});
	}

	/**
	 * 向zk集群注册服务器信息
	 * @param hostname
	 * @throws Exception
	 */
	public void registerServer(String hostname) throws Exception{
		String create = zk.create(parentNode+"server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname + " is online..." + create);
	}

	/**
	 * 业务功能
	 * @throws Exception
	 */
	public void handleBusiness(String hostname) throws Exception{
		System.out.println(hostname + " start working......");
		Thread.sleep(Long.MAX_VALUE);
	}

	public static void main(String[] args) throws Exception {
		//获取zk连接
		DistributedServer server = new DistributedServer();
		server.getConnect();
		//利用zk连接注册服务器信息
		server.registerServer(args[0]);

		//启动业务功能
		server.handleBusiness(args[0]);

	}
}
