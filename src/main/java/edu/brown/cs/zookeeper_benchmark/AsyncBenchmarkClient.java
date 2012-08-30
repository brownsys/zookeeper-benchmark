package edu.brown.cs.zookeeper_benchmark;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.ListenerContainer;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.TestType;

public class AsyncBenchmarkClient extends BenchmarkClient {
	
	TestType _currentType = TestType.UNDEFINED;
	private Boolean _asyncRunning;

	private static final Logger LOG = LoggerFactory.getLogger(AsyncBenchmarkClient.class);


	public AsyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace,
			int attempts, int id) throws IOException {
		super(zkBenchmark, host, namespace, attempts, id);
	}
	
	
	@Override
	protected void submit(int n, TestType type) {
		ListenerContainer<CuratorListener> listeners = (ListenerContainer<CuratorListener>)_client.getCuratorListenable();
		BenchmarkListener listener = new BenchmarkListener(this, _type);
		listeners.addListener(listener);
		_currentType = type;
		
		submitRequests(n, type);
		
		synchronized (_asyncRunning) {
			try {
				_asyncRunning.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		listeners.removeListener(listener);
	}

	private void submitRequests(int n, TestType type) {
		try {
			submitRequestsWrapped(n, type);
		} catch (Exception e) {
			// What can you do? for some reason
			// com.netflix.curator.framework.api.Pathable.forPath() throws Exception
			e.printStackTrace();
		}
	}

	private void submitRequestsWrapped(int n, TestType type) throws Exception {
		byte data[];

		for (int i = 0; i < n; i++) {
			double time = ((double)System.nanoTime() - _zkBenchmark.getStartTime())/1000000000.0;

			switch(type) {
				case READ:
					_client.getData().inBackground(new Double(time)).forPath(_path);
					break;

				case SETSINGLE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().inBackground(new Double(time)).forPath(
							_path, data);
					break;

				case SETMULTI:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().inBackground(new Double(time)).forPath(
							_path + "/" + (_count % _highestN), data);
					break;

				case CREATE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.create().inBackground(new Double(time)).forPath(
							_path + "/" + _count, data);
					_highestN++;
					break;

				case DELETE:
					_client.delete().inBackground(new Double(time)).forPath(_path + "/" + _count);
					_highestDeleted++;

					if (_highestDeleted >= _highestN) {
						zkAdminCommand("stat");
							
						synchronized (_zkBenchmark.getThreadMap()) {
							_zkBenchmark.getThreadMap().remove(new Integer(_id));

							if (_zkBenchmark.getThreadMap().size() == 0) {
								_zkBenchmark.getThreadMap().notify();
							}
						}

						_timer.cancel();
						_count++;
						return;
					}
			}
			_count++;
		}

	}
	
	@Override
	protected void finish() {
		synchronized (_asyncRunning) {
			_asyncRunning.notify();
		}
	}

	@Override
	protected void resubmit(int n) {
		submitRequests(n, _currentType);
	}
}
