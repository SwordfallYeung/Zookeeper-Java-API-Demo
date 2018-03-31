package cn.itcast.zkdist;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author y15079
 * @create 2018-03-17 15:20
 * @desc  分布式应用系统服务器上下线动态感知程序开发  客户端
 **/
public class DistributedClient {

	private static final String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
	private static final int sessionTimeout = 2000;
	private static final String parentNode = "/servers";
	//注意：加volatile的意义何在？保持一致性
	//如果不使用volatile，serverList是保存在堆内存中，多线程访问serverList是把它拷贝各自的栈内存中读取的,这会导致不同线程访问到的serverList不同
	//如果使用volatile，则堆内存中的serverList不会被多线程拷贝到各自的栈内存中，统一访问堆内存中的serverList，保持了一致性
	private volatile List<String> serverList;
	private ZooKeeper zk = null;

	/**
	 * 创建到zk的客户端连接
	 * @throws Exception
	 */
	public void getConnect() throws Exception{
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			public void process(WatchedEvent watchedEvent) {
				//收到事件通知后的回调函数（应该是我们自己的事件处理逻辑）
				try {
					//重新更新服务器列表，并且注册了监听
					getServerList(); //有点像递归，在监听函数里面注册监听，就会不断循环
				} catch (Exception e) {
				}
			}
		});
	}

	/**
	 * 获取服务器信息列表
	 * @throws Exception
	 */
	public void getServerList() throws Exception{
		//获取服务器子节点信息，并且对父节点进行监听
		List<String> children = zk.getChildren(parentNode, true);
		//先创建一个局部的list来存服务器信息
		List<String> servers = new ArrayList<String>();
		for (String child: children){
			//child只是子节点的节点名
			byte[] data = zk.getData(parentNode + "/" + child, false, null);
			servers.add(new String(data));
		}
		//把servers赋值给成员变量serverList，已提供给各业务线程使用
		serverList = servers;

		//打印服务器列表
		System.out.println(serverList);
	}

	/**
	 * 业务功能
	 * @throws Exception
	 */
	public void handleBusiness() throws Exception{
		System.out.println("client start working......");
		Thread.sleep(Long.MAX_VALUE);
	}


	public static void main(String[] args) throws Exception {

		//获取zk连接
		DistributedClient client = new DistributedClient();
		client.getConnect();

		//获取servers的子节点信息（并监听），从中获取服务器信息列表
		client.getServerList();

		//启动业务功能
		client.handleBusiness();
	}
}
