package edu.brown.cs.zookeeper_benchmark;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.testStat;

class BenchmarkListener implements CuratorListener{
	BenchmarkClient _client; // client listener listens for
	testStat _stat;//current test

	BenchmarkListener(BenchmarkClient client, testStat stat) {
		_client = client;
		_stat = stat;
	}

	@Override
	public void eventReceived(CuratorFramework arg0, CuratorEvent arg1)
			throws Exception {

		CuratorEventType type = arg1.getType();

		// Ensure that the event we received applies to current test
		if ((type == CuratorEventType.GET_DATA && this.benchmarkClient.curatorTest._currentTest == testStat.READ) ||
			(type == CuratorEventType.SET_DATA && this.benchmarkClient.curatorTest._currentTest == testStat.SETMUTI) ||
			(type == CuratorEventType.SET_DATA && this.benchmarkClient.curatorTest._currentTest == testStat.SETSINGLE) ||
			(type == CuratorEventType.DELETE && this.benchmarkClient.curatorTest._currentTest == testStat.DELETE) ||
			(type == CuratorEventType.CREATE && this.benchmarkClient.curatorTest._currentTest == testStat.CREATE)) {
				this.benchmarkClient.curatorTest._finishedTotal.incrementAndGet();
				this.benchmarkClient.curatorTest.recordEvent(arg1, this.benchmarkClient._records);
				_client.submitAsync(1, _stat);
		}
					

		/*byte[] d = arg1.getData() ;
		String a = new String(d);
		System.out.println(">"+a+"<");*/
	}			
}