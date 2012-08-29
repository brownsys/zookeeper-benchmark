package edu.brown.cs.zookeeper_benchmark;

import java.io.IOException;
import java.util.Timer;

import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.retry.RetryNTimes;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.TestType;

public class AsyncBenchmarkClient extends BenchmarkClient {
	
	TestType _currentType = TestType.UNDEFINED;

	public AsyncBenchmarkClient(ZooKeeperBenchmark curatorTest, String host, String namespace,
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
		ListenerContainer<CuratorListener> listeners = (ListenerContainer<CuratorListener>)_client.getCuratorListenable();
		BenchmarkListener listener = new BenchmarkListener(this, _type);
		listeners.addListener(listener);
		_currentType = type;
		
		submitRequests(n, type);
		
		synchronized(_timer) {
			_timer.wait();
		}
		listeners.removeListener(listener);
	}

	private void submitRequests(int n, TestType type) throws Exception {
		
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
	
	@Override
	protected void finish() {
		synchronized(_timer) {
			_timer.notify();
		}
	}

	@Override
	protected void resubmit(int n) throws Exception {
		submitRequests(n, _currentType);
	}
	
}
