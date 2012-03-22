import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;


import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.retry.RetryNTimes;


public class curatorTest {

	int attempts;

	int _totalOps;
	int _lowerbound;
	Client[] _clients;
	String[] _hosts;
	int _interval;
	HashMap<Integer, Thread> _running;
	AtomicInteger _finishedTotal;
	int _oldTotal;
	int _timeCounter;
	int _deadline;
	AtomicInteger _curtotalOps;
	long _lastCpuTime;
	long _currCpuTime;
	long _startCpuTime;
	
	
	int _increment;
	testStat _currentTest;
	
	String _data;
	
	boolean _sync;
	
	BufferedWriter _bw;
	
	private enum testStat{
		READ, SETSINGLE, SETMUTI, CREATE, DELETE, CLEANING, UNDEFINED
	}

	private class Client implements Runnable{
		
		String _host;//the host this client is connecting to
		CuratorFramework _client;//the actual client
		testStat _stat;//current test
		int _attempts;
		String _path;
		int _id;
		int count;
		int countTime;
		Timer _timer;
		boolean _syncfin;
		int _highestN;
		
		int getTimeCount(){
			return countTime;
		}
		
		int getOpsCount(){
			return count;
		}
		Client(String host, String namespace, int attempts, int id) throws IOException {
			_host = host;
			_client = CuratorFrameworkFactory.builder()
				.connectString(_host).namespace(namespace)
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE,1000))
				.connectionTimeoutMs(5000).build();
			_stat = testStat.UNDEFINED;
			_attempts = attempts;
			_id = id;
			_path = "/client"+id;
			_timer = new Timer();
			_highestN = 0;
		}
		
		void setStat(testStat stat){
			_stat = stat;
		}
		
		@Override
		public void run(){			
			if(!_client.isStarted())
				_client.start();
			_syncfin = false;
			
			count = 0;
			countTime = 0;	
			
			try{
				Stat stat = _client.checkExists().forPath(_path);
				if(stat == null){
					_client.create().forPath(_path, _data.getBytes());
				}
				
			
				if(_stat != testStat.CLEANING){
					//create the timer
					_timer.scheduleAtFixedRate(new TimerTask(){
						@Override
						public void run() {
							//this can be used to measure rate of each thread
							//at this moment, it is not necessary
							countTime++;
							if(countTime == _deadline){								
								this.cancel();
								if(!_sync){
									synchronized(_timer){
										_timer.notify();
									}
								}else{
									_syncfin = true;
								}
							}
							
						}
					}, _interval, _interval);	
					
					
					if(_sync){
						performSync(_stat);
					}else{
						ListenerContainer<CuratorListener> listeners =
						(ListenerContainer<CuratorListener>)_client.getCuratorListenable();
						Listener listener = new Listener();
						listeners.addListener(listener);
						submitAsync(_attempts, _stat);
						//blocks until awaken by timer
						synchronized(_timer){
							_timer.wait();
						}
						listeners.removeListener(listener);
					}
				}
				else{
					doClean();
				}
				/*stat = _client.checkExists().forPath(_path);
				if(stat != null){
					_client.delete().forPath(_path);
				}*/
			}catch(Exception e){
				e.printStackTrace();
			}

			System.err.println(_id+"-i'm done, reqs:"+count);
			synchronized(_running){
				_running.remove(new Integer(_id));
				if(_running.size() == 0)
					_running.notify();
			}
			
		}
		
		void performSync(testStat type) throws Exception{
			switch(type){
			case READ:
				for(int i = 0 ;i < _curtotalOps.get();i++){
					_client.getData().forPath(_path);
					count ++;
					_finishedTotal.incrementAndGet();
					if(_syncfin)
						break;
				}
				break;
			case SETSINGLE:
				for(int i = 0 ;i < _curtotalOps.get();i++){
					_client.setData().
					forPath(_path,new String(_data+i).getBytes());
					count ++;
					_finishedTotal.incrementAndGet();
					if(_syncfin)
						break;
				}
				break;
			case SETMUTI:
				for(int i = 0 ;i < _curtotalOps.get();i++){
					try{
						_client.setData().
						forPath(_path+"/"+(count%_highestN),new String(_data+i).getBytes());
					}catch(NoNodeException e){
						
					}
					count ++;
					_finishedTotal.incrementAndGet();
					if(_syncfin)
						break;
				}
				break;
			case CREATE:
				for(int i = 0 ;i < _curtotalOps.get();i++){
					_client.create().forPath(_path+"/"+count,new String(_data+i).getBytes());
					count ++;
					_highestN++;
					_finishedTotal.incrementAndGet();
					if(_syncfin)
						break;
				}
				break;
			case DELETE:
				for(int i = 0 ;i < _curtotalOps.get();i++){
					try{
						_client.delete().forPath(_path+"/"+count);
					}catch(NoNodeException e){
						
					}
					count ++;
					_finishedTotal.incrementAndGet();
					if(_syncfin)
						break;
				}
				break;	
			}
		}
		void submitAsync(int n, testStat type) throws Exception{			
			switch(type){
			case READ:
				for(int i = 0 ;i<n;i++){
					_client.getData().inBackground().forPath(_path);
					count++;
				}
				break;
			case SETSINGLE:
				for(int i = 0 ;i<n;i++){
					_client.setData().inBackground().
					forPath(_path,new String(_data+i).getBytes());
					count++;
				}	
				break;
			case SETMUTI:
				for(int i = 0 ;i<n;i++){
					_client.setData().inBackground().
					forPath(_path+"/"+(count%_highestN),new String(_data).getBytes());
					count++;
				}
				break;
			case CREATE:
				for(int i = 0 ;i<n;i++){
					_client.create().inBackground().forPath(_path+"/"+count,new String(_data).getBytes());
					_highestN ++;
					count++;
				}	
				break;
			case DELETE:
				for(int i = 0 ;i<n;i++){
					_client.delete().inBackground().forPath(_path+"/"+count);
					count++;
				}	
				break;				
			}
			//System.out.println(_id+"submit new "+ n);
		}
		
		void doClean() throws Exception{
			List<String> children;
			do{
				children = _client.getChildren().forPath(_path);
				for(String child : children){
					_client.delete().inBackground().forPath(_path+"/"+child);
				}
				Thread.sleep(2000);
			}while(children.size()!=0);
		}
		
		private class Listener implements CuratorListener{

			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				_finishedTotal.incrementAndGet();
			}			
		}
	}

	double getTime(){
		/*return the max time consumed by each thread*/
		double ret = 0;
		for(int i = 0;i<_clients.length;i++){
			if(ret < _clients[i].getTimeCount())
				ret = _clients[i].getTimeCount();
		}
		return (ret * _interval)/1000 ;
	}
	
	int getTotalOps(){
		/*return the total number of reqs done by all threads*/
		int ret = 0;
		for(int i = 0;i<_clients.length;i++){
			ret += _clients[i].getOpsCount();
		}
		return ret;
	}
	
	
	public void doTest(testStat stat) throws InterruptedException{
		_finishedTotal = new AtomicInteger(0);
		_oldTotal = 0;
		_curtotalOps = new AtomicInteger(_totalOps);

		try{
			_bw = new BufferedWriter(new FileWriter(new File(stat+".dat")));
		}catch(IOException e){
			e.printStackTrace();
		}

                _startCpuTime = System.nanoTime();
                _lastCpuTime = _startCpuTime;
	
		for(int i = 0;i<_hosts.length;i++){
			_clients[i].setStat(stat);
			Thread tmp = new Thread(_clients[i]);			
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		_currentTest = stat;
		synchronized(_running){
			_running.wait();
		}
		_currentTest = testStat.UNDEFINED;
		try {
			_bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		double time = getTime();
		System.err.println(stat+" finished, time elapsed(sec):"+time
				+" operations:"+_finishedTotal.get()+" avg rate:"+_finishedTotal.get()/time);
	}
	
	public void launch(int totaltime, boolean sync) throws InterruptedException{		
		_timeCounter = 0;
		_finishedTotal = new AtomicInteger(0);
		_oldTotal = 0;
		_deadline = totaltime / _interval;
		_sync = sync;
/*		_lastCpuTime = System.nanoTime();
		_currCpuTime = _lastCpuTime;
		_startCpuTime = 0; */
		
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				_timeCounter ++;
				if(_currentTest != testStat.UNDEFINED){
					int finished = _finishedTotal.get();
					if(finished == 0){
						//this means even the first batch of operations haven't
						return;
					}
/*					if(_startCpuTime == 0){
						_startCpuTime = System.nanoTime();
						_lastCpuTime = _startCpuTime;
						_currCpuTime = _startCpuTime;						
					} */
					//System.err.println("increment:"+(finished - _oldTotal));
					_currCpuTime = System.nanoTime();
					
					
					String msg = ((double)(_currCpuTime - _startCpuTime)/1000000000.0)+" "
					+((double)(finished - _oldTotal)/((double)(_currCpuTime - _lastCpuTime)/1000000000.0));
					// System.out.println(msg);
					//displayCpuTime(_lastCpuTime);
					//displayCpuTime(_currCpuTime);
					_lastCpuTime = _currCpuTime;
					
					try {
						if (finished - _oldTotal > 0) {
							_bw.write(msg+"\n");
						}
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					_oldTotal = finished;
					if(_curtotalOps.get() - finished <= _lowerbound){
						try{
							int avg = _increment / _clients.length;
							if(!_sync){
								for(int i = 0;i<_clients.length;i++){
									_clients[i].submitAsync(avg, _currentTest);
								}
								_curtotalOps.getAndAdd(_increment);
							}else{
								_curtotalOps.getAndAdd(1000);
							}						
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}
			}			
		}, _interval, _interval);	
		
		
		/*this is where all tests start*/
		
		/*Read requests done done by zookeeper extremely 
		 * fast compared with write requests. If the time
		 * interval and threshold are not chosen appropriately, 
		 * it could happen that when the timer awakes, all requests 
		 * have already been finished. In this case, the output 
		 * of read test doesn't reflect the actual rate of 
		 * read requests. */
		doTest(testStat.READ);

		doTest(testStat.SETSINGLE);
		
		doTest(testStat.CREATE);
		
		doTest(testStat.SETMUTI);
		
		/*In the test, node creation and deletion tests are 
		 * done by creating a lot of nodes at first and then 
		 * deleting them. Since both of these two tests run 
		 * for a certain time, there is no guarantee that which 
		 * requests is more than the other. If there are more 
		 * delete requests than create requests, the extra delete 
		 * requests would end up not actually deleting anything. 
		 * Though these requests are sent and processed by 
		 * zookeeper server anyway, this could still be an issue.*/
		doTest(testStat.DELETE);
		
		System.err.println("tests done cleaning");

		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.CLEANING);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		timer.cancel();
		System.err.println("all finished");
	}	
	
	public curatorTest(String[] hs, int interval, int ops, int lowerbound) throws IOException{
		/*
		 * ops here represents the number of total number of ops submitted to server
		 * say 10000, then if it falls below 2000, submit another 8000 to reach 10000
		 * */
		_totalOps = ops; 
		_lowerbound = lowerbound;
		_hosts = hs;
		_clients = new Client[hs.length];
		_interval = interval;
		int avgOps = ops/hs.length;
		for(int i = 0;i<hs.length;i++){
			_clients[i] = new Client(hs[i], "/zkTest", avgOps, i);
			
		}
		_running = new HashMap<Integer,Thread>();
		_deadline = 0;
		_increment = ops - lowerbound;
		
		_data = "!!!!!";
		for(int i = 0;i<19;i++){
			_data += "!!!!!";
		}
	}	
	
	/*
	 * args[0] is the interval
	 * args[1] is the total number of requests, say 16000 requests are submitted, and 
	 * whenever it is below 4000, submit another 16000 - 4000 requests, here args[1] = 16000
	 * args[2] is the threshold, it is 4000 in above example
	 * args[3] is how much time you want to run the test, in millisecond
	 * args[4] is syn or async test, 0 for async, non-0 for sync
	 */
	public static void main(String[] args) throws Exception{
		
		if(args.length != 5){
			System.out.println("wrong parameters");
		}
		String[] hosts = new String[5];
		hosts[0] = "host1.pane.cs.brown.edu:2181";
		hosts[1] = "host2.pane.cs.brown.edu:2181";
		hosts[2] = "host3.pane.cs.brown.edu:2181";
		hosts[3] = "host4.pane.cs.brown.edu:2181";
		hosts[4] = "host5.pane.cs.brown.edu:2181";
		
		int interval = Integer.parseInt(args[0]);
		int totalnumber = Integer.parseInt(args[1]);
		int threshold = Integer.parseInt(args[2]);
		int time = Integer.parseInt(args[3]);
		boolean sync = Integer.parseInt(args[4]) == 0 ? false : true;
		/*String[] hosts = new String[1];
		hosts[0] = "localhost:2181";*/
		System.err.println(interval+"  "+totalnumber+" "+threshold+" "+time+" "+sync);
		curatorTest test = new curatorTest(hosts, 
				interval, totalnumber, threshold);
		test.launch(time, sync);
		System.exit(0);
		
	}
}
