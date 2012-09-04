package edu.brown.cs.zkbenchmark;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

class BenchmarkListener implements CuratorListener {
	private BenchmarkClient _client; // client listener listens for
	private TestType _type; // type of test we should submit new requests for

	BenchmarkListener(BenchmarkClient client, TestType type) {
		_client = client;
		_type = type;
	}

	@Override
	public void eventReceived(CuratorFramework client, CuratorEvent event) {
		CuratorEventType type = event.getType();

		// Ensure that the event is reply to current test
		if ((type == CuratorEventType.GET_DATA && _client.getBenchmark().getCurrentTest() == TestType.READ) ||
			(type == CuratorEventType.SET_DATA && _client.getBenchmark().getCurrentTest() == TestType.SETMULTI) ||
			(type == CuratorEventType.SET_DATA && _client.getBenchmark().getCurrentTest() == TestType.SETSINGLE) ||
			(type == CuratorEventType.DELETE && _client.getBenchmark().getCurrentTest() == TestType.DELETE) ||
			(type == CuratorEventType.CREATE && _client.getBenchmark().getCurrentTest() == TestType.CREATE)) {
				_client.getBenchmark().incrementFinished();
				_client.recordEvent(event);
				_client.submit(1, _type);
		}
	}			
}
