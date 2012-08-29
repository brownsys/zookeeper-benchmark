package edu.brown.cs.zookeeper_benchmark;

import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException.NoNodeException;

import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.TestType;

public class SyncBenchmarkClient extends BenchmarkClient {

	AtomicInteger _totalOps;
	protected boolean _syncfin;
	
	public SyncBenchmarkClient(ZooKeeperBenchmark curatorTest, String host, String namespace,
			int attempts, int id) throws IOException {
		_curatorTest = curatorTest;
		_host = host;
		_client = CuratorFrameworkFactory.builder()
			.connectString(_host).namespace(namespace)
			.retryPolicy(new RetryNTimes(Integer.MAX_VALUE,1000))
			.connectionTimeoutMs(5000).build();
		_type = TestType.UNDEFINED;
		_attempts = attempts;
		_id = id;
		_path = "/client"+id;
		_timer = new Timer();
		_highestN = 0;
		_highestDeleted = 0;		
	}
	
	@Override
	protected void submit(int n, TestType type) throws Exception {
		
		_syncfin = false;		
		_totalOps = _curatorTest.getCurrentTotalOps();
		
		for(int i = 0 ;i < _totalOps.get();i++) {
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
					_highestN ++;
					break;
				case DELETE:
					try {
						_client.delete().forPath(_path+"/"+count);
					} catch(NoNodeException e) {
						e.printStackTrace();
					}
			}

			recordTimes(new Double(time));

			count ++;
			_curatorTest.incrementFinished();
			if(_syncfin)
				break;
		}
		
	}
	
	@Override
	protected void finish() {
		_syncfin = true;
	}

	/**
	 * in fact, n here can be arbitrary number as synchronous operations can be stopped 
	 * after finishing any operation.
	 */
	@Override
	protected void resubmit(int n) throws Exception {
		_totalOps.getAndAdd(n);
	}

}
