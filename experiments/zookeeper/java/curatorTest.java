import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;


import org.apache.zookeeper.data.Stat;


import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class curatorTest {
	int attempts;
	int thrNum;
	/*int _totalread;
	int _totalsetSingle;
	int _totalsetMutiple;
	int _totalcreate;
	int _totaldelete;
	BufferedWriter[] _bws;
	int[] _rate;
	int[] _tmp;*/

	class clientThread implements Runnable{
		CuratorFramework client;
		int id;
		String path;
		int[] curtry; 
		int[] lasttry;
		boolean samepath;
		int counter;
		double avg;
		int interval;
		String host;
		String data;
		BufferedWriter[] bws;
		
		clientThread(int i, boolean all,int time, String h){
			id = i;
			curtry = new int [5];
			lasttry = new int [5];
			interval = time;
			samepath = all;
			samepath = false;
			counter = 0;
			avg = 0;
			host = h;
			data = "!!!!!";
			for(int l = 0;l<19;l++){
				data += "!!!!!";
			}
			//System.out.println("strlen:"+data.length()+" bytes:"+data.getBytes().length);
		}
		@Override
		public void run() {
			try{
				
				File readFile = new File("test"+id+"-read.txt");
				File setsingleFile = new File("test"+id+"-setSingle.txt");
				File createFile = new File("test"+id+"-create.txt");
				File setmutiFile = new File("test"+id+"-setMuti.txt");
				File deleteFile = new File("test"+id+"-delete.txt");
				bws = new BufferedWriter[5];
				
				if(samepath == true){
					path = "/zkTestForAll";
				}else{
					path = "/zkTest"+id;
				}
				 client = CuratorFrameworkFactory.builder()
				.connectString(host).namespace("/zkTest")
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE,1000))
				.connectionTimeoutMs(5000).build();
				 
				 
				 
				client.start();
				//read test(for reference)
				System.out.println("read data from single node");
				bws[0] = new BufferedWriter(new FileWriter(readFile));
				Timer timer = new Timer();
				double total;
				Stat stat;
				curtry[0] = 0;
				lasttry[0] = 0;
				timer.scheduleAtFixedRate(new TimerTask(){
					public void run(){
						String msg = id+":now have "+curtry[0]+" increased "+(curtry[0] - lasttry[0]);	
						avg = (double)(curtry[0] - lasttry[0])/((double)interval / 1000);
						lasttry[0] = curtry[0];
						counter++;
						//avg = (double)curtry/((double)interval * (double)counter / 1000);
						msg += " rate "+avg+" op/sec ";
						//System.out.println(msg);
						try {
							bws[0].write(avg+"\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
						if(curtry[0] == attempts){
							this.cancel();
						}
					}
				}
				, interval, interval);
				
				
				for(int i = 0;i<attempts;i++){					
					client.getData().forPath("/read");
					//client.getData().watched().inBackground().forPath("read");
					//_totalread ++;
					curtry[0]++;
				}
				
				
				stat = client.checkExists().forPath(path);
				if(stat == null){
					client.create().forPath(path, new byte[0]);
				}			
				total = ((double)interval * (double)counter / 1000);
				System.out.println(id+" finished read to single node:"+attempts
						+" time elapsed:"+total+" sec avg rate:"+curtry[0]/total);
				
				
				Thread.sleep(2000);
				//set test on single znode
				curtry[1] = 0;
				lasttry[1] = 0;
				System.out.println("set data to single node");
				bws[1] = new BufferedWriter(new FileWriter(setsingleFile));
				timer = new Timer();
				timer.scheduleAtFixedRate(new TimerTask(){
					public void run(){
						String msg = id+":now have "+curtry[1]+" increased "+(curtry[1] - lasttry[1]);	
						avg = (double)(curtry[1] - lasttry[1])/((double)interval / 1000);
						lasttry[1] = curtry[1];
						counter++;
						//avg = (double)curtry/((double)interval * (double)couzkTestForAllnter / 1000);
						
						msg += " rate "+avg+" op/sec ";
						try {
							bws[1].write(avg+"\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
						//System.out.println(msg);
						if(curtry[1] == attempts){
							this.cancel();
						}
					}
				}
				, interval, interval);
				
				for(int i = 0;i<attempts;i++){					
					client.setData().forPath(path, data.getBytes());
					//_totalsetSingle ++;
					curtry[1]++;
				}
				
				total = ((double)interval * (double)counter / 1000);
				System.out.println(id+" finished set to single node:"+attempts
						+" time elapsed:"+total+" sec avg rate:"+curtry[1]/total);
				
				
				Thread.sleep(2000);
				//create test
				System.out.println("create");
				bws[2] = new BufferedWriter(new FileWriter(createFile));
				curtry[2] = 0;
				lasttry[2] = 0;
				counter = 0;
				
				timer.scheduleAtFixedRate(new TimerTask(){
					public void run(){
						String msg = id+":now have "+curtry+" increased "+(curtry[2] - lasttry[2]);
						avg = (double)(curtry[2] - lasttry[2])/((double)interval / 1000);
						lasttry[2] = curtry[2];
						counter++;
						//avg = (double)curtry/((double)interval * (double)counter / 1000);
						msg += " rate "+avg+" op/sec ";
						try {
							bws[2].write(avg+"\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
						//System.out.println(msg);
						if(curtry[2] == attempts){	
							this.cancel();
						}
					}
				}
				, interval, interval);
				
				for(int i = 0;i<attempts;i++){					
					String newpath = path +"/num"+curtry[2];
					client.create().forPath(newpath);
					//_totalcreate ++;
					curtry[2] ++;
				}
				total = ((double)interval * (double)counter / 1000);
				System.out.println(id+" finished create:"+attempts
						+" time elapsed:"+total+" sec avg rate:"+curtry[2]/total);
				
				
				Thread.sleep(2000);
				//write data to each node
				System.out.println("set data to multi node");
				bws[3] = new BufferedWriter(new FileWriter(setmutiFile));
				curtry[3] = 0;
				lasttry[3] = 0;
				counter = 0;
				timer.scheduleAtFixedRate(new TimerTask(){
					public void run(){
						String msg = id+":now have "+curtry+" increased "+(curtry[3] - lasttry[3]);
						avg = (double)(curtry[3] - lasttry[3])/((double)interval / 1000);
						lasttry[3] = curtry[3];
						counter++;
						//avg = (double)curtry/((double)interval * (double)counter / 1000);
						msg += " rate "+avg+" op/sec ";
						try {
							bws[3].write(avg+"\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
						//System.out.println(msg);
						if(curtry[3] == attempts){
							this.cancel();
						}
					}
				}
				, interval, interval);
				
				for(int i = 0;i<attempts;i++){					
					String newpath = path +"/num"+curtry[3];
					client.setData().forPath(newpath,data.getBytes());
					//_totalsetMutiple ++;
					curtry[3] ++;
				}
				
				total = ((double)interval * (double)counter / 1000);
				System.out.println(id+" finished write to muti nodes:"+attempts
						+" time elapsed:"+total+" sec avg rate:"+curtry[3]/total);
				
				
				Thread.sleep(2000);	
				
				//delete these nodes
				bws[4] = new BufferedWriter(new FileWriter(deleteFile));
				System.out.println("delete nodes");
				curtry[4] = 0;
				lasttry[4] = 0;
				counter = 0;
				timer = new Timer();
				timer.scheduleAtFixedRate(new TimerTask(){
					public void run(){
						String msg = id+":now have "+curtry+" increased "+(curtry[4] - lasttry[4]);	
						avg = (double)(curtry[4] - lasttry[4])/((double)interval / 1000);
						lasttry[4] = curtry[4];
						counter++;
						//avg = (double)curtry/((double)interval * (double)counter / 1000);
						msg += " rate "+avg+" op/sec ";
						//System.out.println(msg);
						try {
							bws[4].write(avg+"\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
						if(curtry[4] == attempts){	
							this.cancel();
						}
					}
				}
				, interval, interval);
				
				for(int i = 0;i<attempts;i++){
					
					String newpath = path +"/num"+curtry[4];
					client.delete().forPath(newpath);
					//_totaldelete ++;
					curtry[4] ++;
				}
				
				total = ((double)interval * (double)counter / 1000);
				System.out.println(id+" finished delete:"+attempts
						+" time elapsed:"+total+" sec avg rate:"+curtry[4]/total);
				
				stat = client.checkExists().forPath(path);
				if(stat != null){
					client.delete().forPath(path);
				}
				Thread.sleep(2000);
				
				client.close();
				for(int i = 0;i<5;i++)
					bws[i].close();
				System.out.println(id+" all finished");
			}catch(Exception e){
				
			}
			
		}		
	}
	
	public void launch(int n, int a, boolean all, int time, String[] hosts) throws Exception{
		thrNum = n;
		attempts = a;
		/*this.totalcreate = 0;
		this.totaldelete = 0;
		this.totalread = 0;
		this.totalsetMutiple = 0;
		this.totalsetSingle = 0;
		_tmp = new int[5];
		_rate = new int[5];
		_bws = new BufferedWriter[5];
		Timer[] timers = new Timer[5];
		bws[0] = new BufferedWriter(new FileWriter(new File("./newtotalread.txt")));
		bws[1] = new BufferedWriter(new FileWriter(new File("./newtotalsetSingle.txt")));
		bws[2] = new BufferedWriter(new FileWriter(new File("./newtotalcreate.txt")));
		bws[3] = new BufferedWriter(new FileWriter(new File("./newtotalsetMutiple.txt")));
		bws[4] = new BufferedWriter(new FileWriter(new File("./newtotaldelete.txt")));
		for(int i = 0;i<5;i++){
			tmp[i] = 0;
			rate[i] = 0;
		}
		timers[0] = new Timer();
		timers[0].scheduleAtFixedRate(new TimerTask(){
			public void run(){
				if(totalread != 0){
					rate[0] = totalread - tmp[0];
					try {
						bws[0].write(rate[0]+"\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
					tmp[0] = totalread;
				}
			}
		}
		, time, time);
		
		timers[1] = new Timer();
		timers[1].scheduleAtFixedRate(new TimerTask(){
			public void run(){
				if(totalsetSingle != 0){
					rate[1] = totalsetSingle - tmp[1];
					try {
						bws[1].write(rate[1]+"\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
					tmp[1] = totalsetSingle;
				}
			}
		}
		, time, time);
		
		timers[2] = new Timer();
		timers[2].scheduleAtFixedRate(new TimerTask(){
			public void run(){
				if(totalcreate != 0){
					rate[2] = totalcreate - tmp[2];
					try {
						bws[2].write(rate[2]+"\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
					tmp[2] = totalcreate;
				}
			}
		}
		, time, time);
		
		timers[3] = new Timer();
		timers[3].scheduleAtFixedRate(new TimerTask(){
			public void run(){
				if(totalsetMutiple != 0){
					rate[3] = totalsetMutiple - tmp[3];
					try {
						bws[3].write(rate[3]+"\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
					tmp[3] = totalsetMutiple;
				}
			}
		}
		, time, time);
		
		timers[4] = new Timer();
		timers[4].scheduleAtFixedRate(new TimerTask(){
			public void run(){
				if(totaldelete != 0){
					rate[4] = totaldelete - tmp[4];
					try {
						bws[4].write(rate[4]+"\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
					tmp[4] = totaldelete;
				}
			}
		}
		, time, time);
		
		*/
		int numHosts = hosts.length;
		for(int i = 0;i<n;i++){
			clientThread client = new clientThread(i, all, time, hosts[i%numHosts]);
			Thread thread = new Thread(client);
			thread.start();
		}
		
		
	}
	
	public static void main(String[] args) throws Exception{
		curatorTest test = new curatorTest();
		String[] hosts = new String[5];
		hosts[0] = "euc03.cs.brown.edu:2181";
		hosts[1] = "euc04.cs.brown.edu:2181";
		hosts[2] = "euc05.cs.brown.edu:2181";
		hosts[3] = "euc06.cs.brown.edu:2181";
		hosts[4] = "euc07.cs.brown.edu:2181";
		
		/*String[] hosts = new String[1];
		hosts[0] = "localhost:2181";*/
		test.launch(20, 5000, false , 200, hosts);
	}
}