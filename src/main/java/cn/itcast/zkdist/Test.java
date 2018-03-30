package cn.itcast.zkdist;

/**
 * @author y15079
 * @create 2018-03-17 18:23
 * @desc
 **/
public class Test {
	public static void main(String[] args) {
		System.out.println("开始了");

		Thread thread = new Thread(new Runnable() {
			public void run() {
				System.out.println("线程开始了");
				while (true){

				}
			}
		});
		thread.setDaemon(true);//守护线程
		thread.start();

	}
}
