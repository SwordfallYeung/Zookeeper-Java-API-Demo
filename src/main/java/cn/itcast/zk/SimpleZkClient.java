package cn.itcast.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author y15079
 * @create 2018-03-16 16:53
 * @desc  简单的增删改查demo
 **/
public class SimpleZkClient {
	private static final String connectString = "mimi1:2181,mimi2:2181,mini3:2181";
	private static final int sessionTimeout = 2000;

	ZooKeeper zkClient = null;

	@Before
	public void init() throws Exception {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			public void process(WatchedEvent watchedEvent) {
				//收到事件通知后的回调函数（应该是我们自己的事件处理逻辑）
				System.out.println(watchedEvent.getType() + "---" + watchedEvent.getPath());
				try {
					zkClient.getChildren("/", true);
				} catch (Exception e) {
				}
			}
		});
	}

	/**
	 * 数据的增删改查
	 */
	//创建数据节点到zk中
	@Test
	public  void testCreate(String[] args) throws Exception{
		//参数1，要创建的节点的路径 参数2：节点数据 参数3：节点的权限 参数4：节点的类型
		String node = zkClient.create("/idea", "hellozk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		//上传的数据可以是任何类型，但都要转成byte[]
	}

	//判断znode是否存在
	public void testExist() throws Exception{
		//节点元数据
		Stat stat = zkClient.exists("/idea", false);
		System.out.println(stat == null ? "not exist": "exist");
	}

	//获取子节点
	@Test
	public void getChildren() throws Exception{
		List<String> children = zkClient.getChildren("/", true);
		for (String child: children){
			System.out.println(child);
		}
		Thread.sleep(Long.MAX_VALUE);//真正运行时可以注释
	}

	//获取znode的数据
	@Test
	public void getData() throws Exception{
		byte[] data = zkClient.getData("/idea",false, null);
		System.out.println(new String(data));
	}

	//删除znode的数据
	@Test
	public void deleteZnode() throws Exception{
		//参数2：指定要删除的版本，-1表示删除所有版本
		zkClient.delete("/idea", -1);
	}

	//设置znode
	@Test
	public void setData() throws Exception{
		zkClient.setData("/app1","hello".getBytes(), -1);
		byte[] data = zkClient.getData("/app1", false, null);
		System.out.println(new String(data));
	}
}
