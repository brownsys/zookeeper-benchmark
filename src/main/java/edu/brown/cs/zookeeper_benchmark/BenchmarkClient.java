package edu.brown.cs.zookeeper_benchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.retry.RetryNTimes;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.testStat;
//import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.BenchmarkClient.Listener;

class BenchmarkClient implements Runnable{
	
	private ZooKeeperBenchmark _curatorTest;
	private String _host;//the host this client is connecting to
	private CuratorFramework _client;//the actual client
	private testStat _stat;//current test
	private int _attempts;
	private String _path;
	private int _id;
	private int count;
	private int countTime;
	private Timer _timer;
	private boolean _syncfin;
	private int _highestN;
	private int _highestDeleted;
	
	private BufferedWriter _recorder;
	
	int getTimeCount() {
		return countTime;
	}
	
	int getOpsCount(){
		return count;
	}
	
	ZooKeeperBenchmark getBenchmark() {
		return _curatorTest;
	}

	BufferedWriter getRecorder() {
		return _recorder;
	}
	
	BenchmarkClient(ZooKeeperBenchmark curatorTest, String host, String namespace, int attempts, int id) throws IOException {
		_curatorTest = curatorTest;
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
		_highestDeleted = 0;
	}
	
	void setStat(testStat stat) {
		_stat = stat;
	}

	void zkAdminCommand(String cmd) {
		String host = _host.split(":")[0];
		Socket socket = null;
		OutputStream os = null;
		InputStream is = null;	
		
		try {
			socket = new Socket(host, 2181);
			os = socket.getOutputStream();
			is = socket.getInputStream();
			os.write(cmd.getBytes());
			os.flush();
			byte[] b = new byte[1000];
			int len = is.read(b);

			while (len >= 0) {
				System.err.println(_id+" " + cmd + " command:\n" + new String(b, 0, len));
				len = is.read(b);
			}
			
			System.err.println(_id+" " + cmd + " command: done.");
			is.close();
			os.close();
			socket.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {			
		if (!_client.isStarted())
			_client.start();
		
		_syncfin = false;
		
		if (_stat == testStat.CLEANING) {
			try {
				doClean();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			synchronized(_curatorTest.getThreadMap()) {
				_curatorTest.getThreadMap().remove(new Integer(_id));
				if(_curatorTest.getThreadMap().size() == 0)
					_curatorTest.getThreadMap().notify();
			}
			return;
		}
		
		zkAdminCommand("srst");
		
		try {
			_curatorTest.getBarrier().await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		} catch (BrokenBarrierException e1) {
			e1.printStackTrace();
		}
		
		
		count = 0;
		countTime = 0;	
		
		try {
			Stat stat = _client.checkExists().forPath(_path);
			if(stat == null) {
				_client.create().forPath(_path, _curatorTest.getData().getBytes());
			}

			//create the timer
			_timer.scheduleAtFixedRate(new TimerTask() {
				@Override
				public void run() {
					//this can be used to measure rate of each thread
					//at this moment, it is not necessary
					countTime++;
					
					if(countTime == BenchmarkClient.this._curatorTest.getDeadline()) {								
						this.cancel();
						if(!BenchmarkClient.this._curatorTest.isSync()) {
							synchronized(_timer) {
								_timer.notify();
							}
						} else {
							_syncfin = true;
						}
					}
						
				}
			}, _curatorTest.getInterval(), _curatorTest.getInterval());	
			
			try {
				_recorder = new BufferedWriter(new FileWriter(new File(_id+"-"+_stat+"_timings.dat")));
			} catch (IOException e) {
				e.printStackTrace();
			}
				
			if (_curatorTest.isSync()) {
				performSync(_stat);
			} else {
				ListenerContainer<CuratorListener> listeners = (ListenerContainer<CuratorListener>)_client.getCuratorListenable();
				BenchmarkListener listener = new BenchmarkListener( this, _stat);
				listeners.addListener(listener);
				submitAsync(_attempts, _stat);
				//blocks until awaken by timer
				synchronized(_timer) {
					_timer.wait();
				}
				listeners.removeListener(listener);
			}

		} catch(Exception e) {
			e.printStackTrace();
		}

		zkAdminCommand("stat");

		try {
			_recorder.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.err.println(_id+"-i'm done, reqs:"+count);
		synchronized(_curatorTest.getThreadMap()) {
			_curatorTest.getThreadMap().remove(new Integer(_id));
			if (_curatorTest.getThreadMap().size() == 0)
				_curatorTest.getThreadMap().notify();
		}		
	}
	
	void performSync(testStat type) throws Exception {
		for(int i = 0 ;i < _curatorTest.getCurrentTotalOps();i++) {
			double time = ((double)System.nanoTime() - _curatorTest.getStartTime())/1000000000.0;

			switch(type) {
				case READ:
					_client.getData().forPath(_path);
					break;
				case SETSINGLE:
					_client.setData().forPath(_path,new String(_curatorTest.getData() + i).getBytes());
					break;
				case SETMUTI:
					try {
						_client.setData().forPath(_path+"/"+(count%_highestN),new String(_curatorTest.getData() + i).getBytes());
					} catch(NoNodeException e) {
						e.printStackTrace();
					}
					break;
				case CREATE:
					_client.create().forPath(_path+"/"+count,new String(_curatorTest.getData() + i).getBytes());
					_highestN++;
					break;
				case DELETE:
					try {
						_client.delete().forPath(_path+"/"+count);
					} catch(NoNodeException e) {
						e.printStackTrace();
					}
			}

			_curatorTest.recordTimes(new Double(time), _recorder);

			count ++;
			_curatorTest.incrementFinished();
			if(_syncfin)
				break;
		}
	}

	void submitAsync(int n, testStat type) throws Exception {
		for (int i = 0; i < n; i++) {
			double time = ((double)System.nanoTime() - _curatorTest.getStartTime())/1000000000.0;

			switch(type) {
				case READ:
					_client.getData().inBackground(new Double(time)).forPath(_path);
					break;
				case SETSINGLE:
					_client.setData().inBackground(new Double(time)).forPath(_path,
							new String(_curatorTest.getData() + i).getBytes());
					break;
				case SETMUTI:
					_client.setData().inBackground(new Double(time)).forPath(_path+"/"+(count%_highestN),
							new String(_curatorTest.getData()).getBytes());
					break;
				case CREATE:
					_client.create().inBackground(new Double(time)).forPath(_path+"/"+count,
							new String(_curatorTest.getData()).getBytes());
					_highestN++;
					break;
				case DELETE:
					_client.delete().inBackground(new Double(time)).forPath(_path+"/"+count);
					_highestDeleted++;

					if(_highestDeleted >= _highestN) {
						zkAdminCommand("stat");
							
						synchronized(_curatorTest.getThreadMap()) {
							_curatorTest.getThreadMap().remove(new Integer(_id));
							if(_curatorTest.getThreadMap().size() == 0)
								_curatorTest.getThreadMap().notify();
						}

						_timer.cancel();
						count++;
						return;
					}
			}

			count++;
		}
	}
	
	void doClean() throws Exception {
		List<String> children;
		do {
			children = _client.getChildren().forPath(_path);
			for(String child : children) {
				_client.delete().inBackground().forPath(_path+"/"+child);
			}
			Thread.sleep(2000);
		} while(children.size()!=0);
	}
}
