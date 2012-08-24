import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.zookeeper.data.Stat;


import com.google.common.base.Function;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.retry.RetryNTimes;


public class curatorTestSync {
	/*
	 * each server will have a client submitting all kinds of requests to it,
	 * therefore, one thread per server
	 * */
	int attempts;

	int _totalOps;
	syncClient[] _clients;
	String[] _hosts;
	int _interval;
	HashMap<Integer, Thread> _running;
	AtomicInteger _totalRead;	
	AtomicInteger _totalSetSingle;	
	AtomicInteger _totalSetMuti;	
	AtomicInteger _totalCreate;	
	AtomicInteger _totalDelete;
	int _oldReadVal;
	int _oldSetSVal;
	int _oldSetMVal;
	int _oldCreateVal;
	int _oldDeleteVal;
	int _timeCounter;
	
	int _numThread;

	
	private enum testStat{
		READ, SETSINGLE, SETMUTI, CREATE, DELETE, UNDEFINED
	}
	private class syncClient implements Runnable{
		
		String _host;//the host this client is connecting to
		CuratorFramework _client;//the actual client
		testStat _stat;//current test
		int _localcounter;
		int _attempts;
		String _path;
		int _id;
		int count;
		int lastcount;
		int countTime;
		Timer _timer;
		BufferedWriter bw;
		
		int getTimeCount(){
			return countTime;
		}
		
		syncClient(String host, String namespace, int attempts, int id) throws IOException {
			_host = host;
			_client = CuratorFrameworkFactory.builder()
				.connectString(host).namespace(namespace)
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE,1000))
				.connectionTimeoutMs(5000).build();
			_stat = testStat.UNDEFINED;
			_localcounter = 0;
			_attempts = attempts;
			_id = id;
			_path = "/client"+id;
			_timer = new Timer();
		}
		
		void setStat(testStat stat){
			_stat = stat;
		}
		
		@Override
		public void run(){
			
			if(!_client.isStarted())
				_client.start();
			
			try{
				
				File file = new File(_id+"-"+_stat+"-sync");
				bw = new BufferedWriter(new FileWriter(file));			
				
				
				Stat stat = _client.checkExists().forPath(_path);
				if(stat == null){
					_client.create().forPath(_path, new byte[0]);
				}
				
				count = 0;
				lastcount = 0;
				countTime = 0;
				
				_timer.scheduleAtFixedRate(new TimerTask(){

					@Override
					public void run() {
						double rate = (double)(count - lastcount) / ( (double)_interval /1000);
						/*System.out.println(_id+" current "+(count - lastcount)+" count "+count +
								" countTime:"+countTime+" rate:"+rate);*/
						try {
							bw.write(rate+"\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
						countTime++;
						lastcount = count;
						
						if(count == _attempts){
							this.cancel();
							_timer.purge();
							synchronized(_timer){
								_timer.notify();
							}
						}
					}
				}, _interval, _interval);
				
				switch(_stat){
				case READ:				
					for(int i = 0 ;i<_attempts;i++){
						_client.getData().forPath(_path);
						count++;
						_totalRead.incrementAndGet();
					}	
					synchronized(_timer){
						_timer.wait();
					}	
					break;
				case SETSINGLE:			
					for(int i = 0 ;i<_attempts;i++){
						_client.setData().
						forPath(_path,new String("data"+i).getBytes());
						count++;
						_totalSetSingle.incrementAndGet();
					}	
					synchronized(_timer){
						_timer.wait();
					}	
					break;
				case SETMUTI:
					for(int i = 0 ;i<_attempts;i++){
						_client.setData().
						forPath(_path+"/"+i,new String("data"+i).getBytes());
						count++;
						_totalSetMuti.incrementAndGet();
					}
					synchronized(_timer){
						_timer.wait();
					}	
					break;
				case CREATE:			
					for(int i = 0 ;i<_attempts;i++){
						_client.create().forPath(_path+"/"+i,new byte[0]);
						count++;
						_totalCreate.incrementAndGet();
					}	
					synchronized(_timer){
						_timer.wait();
					}	
					break;
				case DELETE:			
					for(int i = 0 ;i<_attempts;i++){
						_client.delete().forPath(_path+"/"+i);
						count++;
						_totalDelete.incrementAndGet();
					}		
					synchronized(_timer){
						_timer.wait();
					}	
					break;
				case UNDEFINED:
					break;
				}
				/*stat = _client.checkExists().forPath(_path);
				if(stat != null){
					_client.delete().forPath(_path);
				}*/
				bw.close();
			}catch(Exception e){
				e.printStackTrace();
			}

			synchronized(_running){
				_running.remove(new Integer(_id));
				if(_running.size() == 0)
					_running.notify();
			}
		}


	}
	
	
	public curatorTestSync(String[] hs, int interval, int ops, int num) throws IOException{
		_totalOps = ops;
		_hosts = hs;		
		_interval = interval;
		_numThread = num;
		_clients = new syncClient[_numThread];
		int avgOps = ops/hs.length;
		for(int i = 0;i<_numThread;i++){
			_clients[i] = new syncClient(hs[i%hs.length], "/zkTest", avgOps, i);
			
		}
		_running = new HashMap<Integer,Thread>();
		
	}
	
	double getTime(){
		double ret = 0;
		for(int i = 0;i<_clients.length;i++){
			if(ret < _clients[i].getTimeCount())
				ret = _clients[i].getTimeCount();
		}
		return (ret * _interval)/1000 ;
	}
	public void launch() throws InterruptedException{
		
		_timeCounter = 0;
		_totalCreate = new AtomicInteger(0);
		_totalDelete = new AtomicInteger(0);
		_totalRead = new AtomicInteger(0);
		_totalSetMuti = new AtomicInteger(0);
		_totalSetSingle = new AtomicInteger(0);
		_oldSetSVal = -1;
		_oldSetMVal = -1;
		_oldReadVal = -1;
		_oldDeleteVal  = -1;
		_oldCreateVal = -1;
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				int tmp;
				tmp = _totalCreate.get();
				if(tmp != 0 && tmp != _oldCreateVal){
					System.out.println("create, cur rate:"
							+(double)(tmp - _oldCreateVal)/_interval*1000);
					_oldCreateVal = tmp;
				}
				
				tmp = _totalSetSingle.get();
				if(tmp != 0 && tmp != _oldSetSVal){
					System.out.println("set single, cur rate:"
							+(double)(tmp - _oldSetSVal)/_interval*1000);
					_oldSetSVal = tmp;					
				}
				
				tmp = _totalSetMuti.get();
				if(tmp != 0 && tmp != _oldSetMVal){
					System.out.println("set muti, cur rate:"
							+(double)(tmp - _oldSetMVal)/_interval*1000);
					_oldSetMVal = tmp;					
				}
				
				tmp = _totalRead.get();
				if(tmp != 0 && tmp != _oldReadVal){
					System.out.println("read, cur rate:"
							+(double)(tmp - _oldReadVal)/_interval*1000);
					 _oldReadVal = tmp;					
				}
				
				tmp = _totalDelete.get();
				if(tmp != 0 && tmp != _oldDeleteVal){
					System.out.println("delete, cur rate:"
							+(double)(tmp - _oldDeleteVal)/_interval*1000);
					_oldDeleteVal = tmp;					
				}
			}			
		}, _interval, _interval);
		
		for(int i = 0;i<_hosts.length;i++){
			Thread tmp = new Thread(_clients[i]);
			_clients[i].setStat(testStat.READ);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		double time = getTime();
		System.out.println("read finished, time elapsed(sec):"+time
				+" operations:"+_totalOps+" avg rate:"+_totalOps/time);
		
		for(int i = 0;i<_hosts.length;i++){
			Thread tmp = new Thread(_clients[i]);
			_clients[i].setStat(testStat.SETSINGLE);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		time = getTime();
		System.out.println("setSingle finished, time elapsed(sec):"+time
				+" operations:"+_totalOps+" avg rate:"+_totalOps/time);
		
		for(int i = 0;i<_hosts.length;i++){
			Thread tmp = new Thread(_clients[i]);
			_clients[i].setStat(testStat.CREATE);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		time = getTime();
		System.out.println("create finished, time elapsed(sec):"+time
				+" operations:"+_totalOps+" avg rate:"+_totalOps/time);
		
		for(int i = 0;i<_hosts.length;i++){
			Thread tmp = new Thread(_clients[i]);
			_clients[i].setStat(testStat.SETMUTI);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		time = getTime();
		System.out.println("setMuti finished, time elapsed(sec):"+time
				+" operations:"+_totalOps+" avg rate:"+_totalOps/time);
		
		for(int i = 0;i<_hosts.length;i++){
			Thread tmp = new Thread(_clients[i]);
			_clients[i].setStat(testStat.DELETE);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		time = getTime();
		System.out.println("delete finished, time elapsed(sec):"+time
				+" operations:"+_totalOps+" avg rate:"+_totalOps/time);
		
		System.out.println("all finished");
	}
	
	public static void main(String[] args) throws Exception{
		
		String[] hosts = new String[5];
		hosts[0] = "euc03.cs.brown.edu:2181";
		hosts[1] = "euc04.cs.brown.edu:2181";
		hosts[2] = "euc05.cs.brown.edu:2181";
		hosts[3] = "euc06.cs.brown.edu:2181";
		hosts[4] = "euc07.cs.brown.edu:2181";
		
		/*String[] hosts = new String[1];
		hosts[0] = "localhost:2181";*/
		curatorTestSync test = new curatorTestSync(hosts, 200, 10000, 5);
		test.launch();
		
	}
}