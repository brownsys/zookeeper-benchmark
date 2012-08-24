import java.io.IOException;

import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.utils.DefaultTracerDriver;


public class curatorTestDriver {
	CuratorFramework client;
	String host = "euc03.cs.brown.edu:2181";
	
	public void test() throws Exception{
		 client = CuratorFrameworkFactory.builder()
			.connectString(host).namespace("/zkTest2")
			.retryPolicy(new RetryNTimes(Integer.MAX_VALUE,1000))
			.connectionTimeoutMs(5000).build();
		 
		
		 client.start();
		 TimeTrace trace = client.getZookeeperClient().startTracer("trace0");
		 
		 for(int i = 0;i<500;i++){
			 client.setData().forPath("/test",(new String("data"+i)).getBytes());
			 trace.commit();
		 }
		 client.close();
		 System.out.println("finished");
		 
	}
	
	public static void main(String[] arg) throws Exception{
		curatorTestDriver test = new curatorTestDriver();
		test.test();
	}
}
