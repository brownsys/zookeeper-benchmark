import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.zookeeper.data.Stat;


import com.google.common.base.Function;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.retry.RetryNTimes;


public class curatorTestAsyncTime {
	/*
	 * each server will have a client submitting all kinds of requests to it,
	 * therefore, one thread per server
	 * */
	int attempts;

	int _totalOps;
	int _lowerbound;
	asyncClient[] _clients;
	String[] _hosts;
	int _interval;
	HashMap<Integer, Thread> _running;
	AtomicInteger _finishedRead;	
	AtomicInteger _finishedSetSingle;	
	AtomicInteger _finishedSetMuti;	
	AtomicInteger _finishedCreate;	
	AtomicInteger _finishedDelete;
	int _oldReadVal;
	int _oldSetSVal;
	int _oldSetMVal;
	int _oldCreateVal;
	int _oldDeleteVal;
	int _timeCounter;
	int _deadline;
	progress _READ;
	progress _SETSINGLE;
	progress _SETMUTI;
	progress _CREATE;
	progress _DELETE;
	int _totalREAD;
	int _totalSETS;
	int _totalSETM;
	int _totalCREATE;
	int _totalDELETE;
	
	int _highestN;
	int _deleteN;
	int _curset;
	
	int _increment;
	
	String _data;
	
	private enum testStat{
		READ, SETSINGLE, SETMUTI, CREATE, DELETE, CLEANING, UNDEFINED
	}
	
	private enum progress{
		DONE, RUNNING, NOTSTART
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
				if(_stat != testStat.CLEANING){
					_timer.scheduleAtFixedRate(new TimerTask(){
						@Override
						public void run() {
							double rate = (double)(count - lastcount) / ( (double)_interval /1000);
							/*System.out.println(_id+" current "+(count - lastcount)+" count "+count +
							 *" countTime:"+countTime+" rate:"+rate);*/
							try {
								bw.write(rate+"\n");
							} catch (IOException e) {
								e.printStackTrace();
							}
							countTime++;
							lastcount = count;
							if(countTime == _deadline){
								this.cancel();
								_timer.purge();
								synchronized(_timer){
									_timer.notify();
								}
							}						
						}
					}, _interval, _interval);
				}
				switch(_stat){
				case READ:
					readListener readl = new readListener();
					listeners.addListener(readl);				
					submit(_attempts, testStat.READ);
					synchronized(_timer){
						_timer.wait();
					}						
					listeners.removeListener(readl);
					break;
				case SETSINGLE:
					setSingleListener setSinglel = new setSingleListener();
					listeners.addListener(setSinglel);					
					submit(_attempts, testStat.SETSINGLE);
					synchronized(_timer){
						_timer.wait();
					}	
					listeners.removeListener(setSinglel);
					break;
				case SETMUTI:
					setMultiListener setMutil = new setMultiListener();
					listeners.addListener(setMutil);					
					submit(_attempts, testStat.SETMUTI);
					synchronized(_timer){
						_timer.wait();
					}	
					listeners.removeListener(setMutil);
					break;
				case CREATE:
					createListener createl = new createListener();
					listeners.addListener(createl);					
					submit(_attempts, testStat.CREATE);
					synchronized(_timer){
						_timer.wait();
					}	
					listeners.removeListener(createl);
					break;
				case DELETE:
					deleteListener deletel = new deleteListener();
					listeners.addListener(deletel);		 			
					submit(_attempts, testStat.DELETE);	
					synchronized(_timer){
						_timer.wait();
					}	
					listeners.removeListener(deletel);
					break;
				case CLEANING:
					List<String> children;
					do{
						children = _client.getChildren().forPath(_path);
						for(String child : children){
							_client.delete().inBackground().forPath(_path+"/"+child);
						}
					}while(children.size() != 0);
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

			//System.out.println(_id+"-i'm done");
			synchronized(_running){
				_running.remove(new Integer(_id));
				if(_running.size() == 0)
					_running.notify();
			}
		}
		
		void submit(int n, testStat type) throws Exception{
			
			switch(type){
			case READ:
				for(int i = 0 ;i<n;i++){
					_client.getData().inBackground().forPath(_path);
				}
				break;
			case SETSINGLE:
				for(int i = 0 ;i<n;i++){
					_client.setData().inBackground().
					forPath(_path,new String(_data+i).getBytes());
				}	
				break;
			case SETMUTI:
				for(int i = 0 ;i<n;i++){
					_client.setData().inBackground().
					forPath(_path+"/"+_curset,new String(_data+i).getBytes());
					_curset++;
					if(_curset > _totalSETM)
						_curset = _totalSETM; 
				}
				break;
			case CREATE:
				for(int i = 0 ;i<n;i++){
					_client.create().inBackground().forPath(_path+"/"+_highestN,new byte[0]);
					_highestN ++;
				}	
				break;
			case DELETE:
				for(int i = 0 ;i<n;i++){
					_client.delete().inBackground().forPath(_path+"/"+_deleteN);
					_deleteN ++;
				}	
				break;		
				
			}
			//System.out.println(_id+"submit new "+ n);
		}
		

		private class readListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				if(arg1.getType() == CuratorEventType.GET_DATA && _READ == progress.RUNNING){
					count++;
					_finishedRead.incrementAndGet();
				}
				//byte[] data = arg1.getData();
				//System.out.println(new String(data));
			}
		}
		private class createListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				if(arg1.getType() == CuratorEventType.CREATE && _CREATE == progress.RUNNING){
					count++;
					_finishedCreate.incrementAndGet();
				}
			}
		}
		private class setSingleListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				if(arg1.getType() == CuratorEventType.SET_DATA && _SETSINGLE == progress.RUNNING){
					count++;
					_finishedSetSingle.incrementAndGet();
				}
			}
		}
		private class setMultiListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				if(arg1.getType() == CuratorEventType.SET_DATA && _SETMUTI == progress.RUNNING){
					count++;
					_finishedSetMuti.incrementAndGet();
				}
			}
		}
		private class deleteListener implements CuratorListener{
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
					throws Exception {
				if(arg1.getType() == CuratorEventType.DELETE && _DELETE == progress.RUNNING){
					count++;
					_finishedDelete.incrementAndGet();
				}
			}
		}
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
	
	public void launch(int totaltime) throws InterruptedException{
		
		_timeCounter = 0;
		_finishedCreate = new AtomicInteger(0);
		_finishedDelete = new AtomicInteger(0);
		_finishedRead = new AtomicInteger(0);
		_finishedSetMuti = new AtomicInteger(0);
		_finishedSetSingle = new AtomicInteger(0);
		_oldSetSVal = -1;
		_oldSetMVal = -1;
		_oldReadVal = -1;
		_oldDeleteVal  = -1;
		_oldCreateVal = -1;
		_deadline = totaltime / _interval;
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				int tmp;
				_timeCounter ++;
				tmp = _finishedCreate.get();
				if(_CREATE == progress.RUNNING && tmp != 0){
					System.out.println("create:"
							+(double)(tmp - _oldCreateVal)/_interval*1000);
					_oldCreateVal = tmp;
					if(_totalCREATE - tmp <= _lowerbound){
						//System.out.println("_lowerbound:"+_lowerbound+" _tmp:"+tmp+" _totalC:"+_totalCREATE);
						try{
							int avg = _increment / _clients.length;
							//System.out.println("total:"+_totalCREATE+"tmp:"+tmp+" avg:"+avg);
							for(int i = 0;i<_clients.length;i++){
								_clients[i].submit(avg, testStat.CREATE);
							}
							_totalCREATE += _increment;
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}
				
				tmp = _finishedSetSingle.get();
				if(_SETSINGLE == progress.RUNNING && tmp != 0){
					System.out.println("setsingle:"
							+(double)(tmp - _oldSetSVal)/_interval*1000);
					_oldSetSVal = tmp;	
					if(_totalSETS - tmp <= _lowerbound){
						try{
							int avg = _increment / _clients.length;
							//System.out.println("total:"+_totalSETS+"tmp:"+tmp+" avg:"+avg);
							for(int i = 0;i<_clients.length;i++){
								_clients[i].submit(avg, testStat.SETSINGLE);
							}
							_totalSETS += _increment;
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}
				
				tmp = _finishedSetMuti.get();
				if(_SETMUTI == progress.RUNNING && tmp != 0){
					System.out.println("setmuti:"
							+(double)(tmp - _oldSetMVal)/_interval*1000);
					_oldSetMVal = tmp;		
					if(_totalSETM - tmp <= _lowerbound){						
						try{
							int avg = _increment / _clients.length;
							//System.out.println("total:"+_totalSETM+"tmp:"+tmp+" avg:"+avg);
							for(int i = 0;i<_clients.length;i++){
								_clients[i].submit(avg, testStat.SETMUTI);
							}
							_totalSETM += _increment;
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}
				
				tmp = _finishedRead.get();
				if(_READ == progress.RUNNING && tmp != 0){
					System.out.println("read:"
							+(double)(tmp - _oldReadVal)/_interval*1000);
					 _oldReadVal = tmp;	
					 if(_totalREAD - tmp <= _lowerbound){
						 try{
							 int avg = _increment / _clients.length;
							 //System.out.println("total:"+_totalREAD+"tmp:"+tmp+" avg:"+avg);
							 for(int i = 0;i<_clients.length;i++){
								 _clients[i].submit(avg, testStat.READ);
							 }
							 _totalREAD += _increment;
						 }catch(Exception e){
							 e.printStackTrace();
						 }
					 }
				}
				
				tmp = _finishedDelete.get();
				if(_DELETE == progress.RUNNING && tmp != 0){
					System.out.println("delete:"
							+(double)(tmp - _oldDeleteVal)/_interval*1000);
					_oldDeleteVal = tmp;
					if(_totalDELETE - tmp <= _lowerbound){
						try{
							int avg = _increment / _clients.length;
							//System.out.println("total:"+_totalDELETE+"tmp:"+tmp+" avg:"+avg);
							for(int i = 0;i<_clients.length;i++){
								_clients[i].submit(avg, testStat.DELETE);
							}
							_totalDELETE += _increment;
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}
			}			
		}, _interval, _interval);			
		
		for(int i = 0;i<_hosts.length;i++){
			_clients[i].setStat(testStat.READ);
			Thread tmp = new Thread(_clients[i]);			
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		_READ = progress.RUNNING;
		synchronized(_running){
			_running.wait();
		}
		_READ = progress.DONE;
		
		double time = getTime();
		System.out.println("read finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
		
		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.SETSINGLE);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		_SETSINGLE = progress.RUNNING;
		synchronized(_running){
			_running.wait();
		}
		_SETSINGLE = progress.DONE;
		
		time = getTime();
		System.out.println("setSingle finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
		
		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.CREATE);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		_CREATE = progress.RUNNING;
		synchronized(_running){
			_running.wait();
		}
		_CREATE = progress.DONE;
		
		time = getTime();
		System.out.println("create finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
		
		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.SETMUTI);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		_SETMUTI = progress.RUNNING;
		synchronized(_running){
			_running.wait();
		}
		_SETMUTI = progress.DONE;
		
		time = getTime();
		System.out.println("setMuti finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);
		
		
		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.DELETE);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		_DELETE = progress.RUNNING;
		synchronized(_running){
			_running.wait();
		}
		_DELETE = progress.DONE;
		
		time = getTime();
		System.out.println("delete finished, time elapsed(sec):"+time
				+" operations:"+getTotalOps()+" avg rate:"+getTotalOps()/time);	
		
		System.out.println("tests done cleaning");

		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.CLEANING);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		System.out.println("all finished");
	}	
	
	public curatorTestAsyncTime(String[] hs, int interval, int ops, int lowerbound) throws IOException{
		/*
		 * ops here represents the number of total number of ops submitted to server
		 * say 10000, then if it falls below 2000, submit another 8000 to reach 10000
		 * */
		_totalOps = ops; 
		_lowerbound = lowerbound;
		_hosts = hs;
		_clients = new asyncClient[hs.length];
		_interval = interval;
		_highestN = 0;
		_deleteN = 0;
		_curset = 0;
		int avgOps = ops/hs.length;
		for(int i = 0;i<hs.length;i++){
			_clients[i] = new asyncClient(hs[i], "/zkTest", avgOps, i);
			
		}
		_running = new HashMap<Integer,Thread>();
		_deadline = 0;
		_READ = progress.NOTSTART;
		_CREATE = progress.NOTSTART;
		_SETMUTI = progress.NOTSTART;
		_SETSINGLE = progress.NOTSTART;
		_CREATE = progress.NOTSTART;
		_totalREAD = ops;
		_totalCREATE = ops;
		_totalSETS = ops;
		_totalSETM = ops;
		_totalDELETE = ops;
		_increment = ops - lowerbound;
		
		_data = "!!!!!";
		for(int i = 0;i<19;i++){
			_data += "!!!!!";
		}
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
		curatorTestAsyncTime test = new curatorTestAsyncTime(hosts, 200, 12000, 4000);
		test.launch(10000);
		
	}
}