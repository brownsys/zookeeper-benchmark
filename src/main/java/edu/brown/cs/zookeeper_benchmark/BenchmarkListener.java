package edu.brown.cs.zookeeper_benchmark;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.TestType;

class BenchmarkListener implements CuratorListener{
	private BenchmarkClient _client; // client listener listens for
	private TestType _type;//current test

	BenchmarkListener(BenchmarkClient client, TestType type) {
		_client = client;
		_type = type;
	}

	@Override
	public void eventReceived(CuratorFramework client, CuratorEvent event)
			throws Exception {

		CuratorEventType type = event.getType();

		// Ensure that the event is reply to current test
		if ((type == CuratorEventType.GET_DATA && _client.getBenchmark().getCurrentTest() == TestType.READ) ||
			(type == CuratorEventType.SET_DATA && _client.getBenchmark().getCurrentTest() == TestType.SETMUTI) ||
			(type == CuratorEventType.SET_DATA && _client.getBenchmark().getCurrentTest() == TestType.SETSINGLE) ||
			(type == CuratorEventType.DELETE && _client.getBenchmark().getCurrentTest() == TestType.DELETE) ||
			(type == CuratorEventType.CREATE && _client.getBenchmark().getCurrentTest() == TestType.CREATE)) {
				_client.getBenchmark().incrementFinished();
				_client.recordEvent(event);
				_client.submit(1, _type);
		}
	}			
}
