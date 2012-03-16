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


public class curatorTestAsync {
	/*
	 * each server will have a client submitting all kinds of requests to it,
	 * therefore, one thread per server
	 * */
	int attempts;

	int _totalOps;
	asyncClient[] _clients;
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

	
	private enum testStat{
		READ, SETSINGLE, SETMUTI, CREATE, DELETE, UNDEFINED
	}
	private class asyncClient implements Runnable{
		
		String _host;//the host this client is connecting to
		CuratorFramework _client;//the actual client
		testStat _stat;//current test
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
		
		int getOpsCount(){
			return count;
		}
		asyncClient(String host, String namespace, int attempts, int id) throws IOException {
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
		}
		
		void setStat(testStat stat){
			_stat = stat;
		}
		
		@Override
		public void run(){
			
			if(!_client.isStarted())
				_client.start();
			
			try{
				
				File file = new File(_id+"-"+_stat);
				bw = new BufferedWriter(new FileWriter(file));			
				
				
				Stat stat = _client.checkExists().forPath(_path);
				if(stat == null){
					_client.create().forPath(_path, new byte[0]);
				}
				
				count = 0;
				lastcount = 0;
				countTime = 0;
				
				ListenerContainer<CuratorListener> listeners = 
					(ListenerContainer<CuratorListener>)_client.getCuratorListenable();
				
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
					readListener readl = new readListener();
					listeners.addListener(readl);				
					for(int i = 0 ;i<_attempts;i++){
						_client.getData().inBackground().forPath(_path);
					}	
					synchronized(_timer){
						_timer.wait();
					}						
					listeners.removeListener(readl);
					break;
				case SETSINGLE:
					setSingleListener setSinglel = new setSingleListener();
					listeners.addListener(setSinglel);					
					for(int i = 0 ;i<_attempts;i++){
						_client.setData().inBackground().
						forPath(_path,new String("data"+i).getBytes());
					}	
					synchronized(_timer){
						_timer.wait();
					}	
					listeners.removeListener(setSinglel);
					break;
				case SETMUTI:
					setMultiListener setMutil = new setMultiListener();
					listeners.addListener(setMutil);					
					for(int i = 0 ;i<_attempts;i++){
						_client.setData().inBackground().
						forPath(_path+"/"+i,new String("data"+i).getBytes());
					}
					synchronized(_timer){
						_timer.wait();
					}	
					listeners.removeListener(setMutil);
					break;
				case CREATE:
					createListener createl = new createListener();
					listeners.addListener(createl);					
					for(int i = 0 ;i<_attempts;i++){
						_client.create().inBackground().forPath(_path+"/"+i,new byte[0]);
					}	
					synchronized(_timer){
						_timer.wait();
					}	
					listeners.removeListener(createl);
					break;
				case DELETE:
					deleteListener deletel = new deleteListener();
					listeners.addListener(deletel);					
					for(int i = 0 ;i<_attempts;i++){
						_client.delete().inBackground().forPath(_path+"/"+i);
					}		
					synchronized(_timer){
						_timer.wait();
					}	
					listeners.removeListener(deletel);
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


		
		
		private class readListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				count++;
				_totalRead.incrementAndGet();
				//byte[] data = arg1.getData();
				//System.out.println(new String(data));
			}
		}
		private class createListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				count++;
				_totalCreate.incrementAndGet();
			}
		}
		private class setSingleListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				count++;
				_totalSetSingle.incrementAndGet();
			}
		}
		private class setMultiListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				count++;
				_totalSetMuti.incrementAndGet();
			}
		}
		private class deleteListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				count++;
				_totalDelete.incrementAndGet();
			}
		}
	}
	
	
	public curatorTestAsync(String[] hs, int interval, int ops) throws IOException{
		_totalOps = ops;
		_hosts = hs;
		_clients = new asyncClient[hs.length];
		_interval = interval;
		int avgOps = ops/hs.length;
		for(int i = 0;i<hs.length;i++){
			_clients[i] = new asyncClient(hs[i], "/zkTest", avgOps, i);
			
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
	
	int getTotalOps(){
		int ret = 0;
		for(int i = 0;i<_clients.length;i++){
			ret += _clients[i].getOpsCount();
		}
		return ret;
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
			_clients[i].setStat(testStat.READ);
			Thread tmp = new Thread(_clients[i]);			
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		double time = getTime();
		System.out.println("read finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.SETSINGLE);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		time = getTime();
		System.out.println("setSingle finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.CREATE);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		time = getTime();
		System.out.println("create finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.SETMUTI);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		time = getTime();
		System.out.println("setMuti finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.DELETE);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		
		time = getTime();
		System.out.println("delete finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
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
		curatorTestAsync test = new curatorTestAsync(hosts, 200, 100000);
		test.launch();
		
	}
}