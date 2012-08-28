package edu.brown.cs.zookeeper_benchmark;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.testStat;

class BenchmarkListener implements CuratorListener{
	private BenchmarkClient _client; // client listener listens for
	private testStat _stat;//current test

	BenchmarkListener(BenchmarkClient client, testStat stat) {
		_client = client;
		_stat = stat;
	}

	@Override
	public void eventReceived(CuratorFramework client, CuratorEvent event)
			throws Exception {

		CuratorEventType type = event.getType();

		// Ensure that the event is reply to current test
		if ((type == CuratorEventType.GET_DATA && _client.getBenchmark().getCurrentTest() == testStat.READ) ||
			(type == CuratorEventType.SET_DATA && _client.getBenchmark().getCurrentTest() == testStat.SETMUTI) ||
			(type == CuratorEventType.SET_DATA && _client.getBenchmark().getCurrentTest() == testStat.SETSINGLE) ||
			(type == CuratorEventType.DELETE && _client.getBenchmark().getCurrentTest() == testStat.DELETE) ||
			(type == CuratorEventType.CREATE && _client.getBenchmark().getCurrentTest() == testStat.CREATE)) {
				_client.getBenchmark().incrementFinished();
				_client.getBenchmark().recordEvent(event, _client.getRecorder());
				_client.submitAsync(1, _stat);
		}
	}			
}
