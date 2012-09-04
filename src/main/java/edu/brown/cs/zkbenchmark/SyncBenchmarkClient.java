package edu.brown.cs.zkbenchmark;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.log4j.Logger;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class SyncBenchmarkClient extends BenchmarkClient {

	AtomicInteger _totalOps;
	private boolean _syncfin;

	private static final Logger LOG = Logger.getLogger(SyncBenchmarkClient.class);

	
	public SyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace,
			int attempts, int id) throws IOException {
		super(zkBenchmark, host, namespace, attempts, id);
	}
	
	@Override
	protected void submit(int n, TestType type) {
		try {
			submitWrapped(n, type);
		} catch (Exception e) {
			// What can you do? for some reason
			// com.netflix.curator.framework.api.Pathable.forPath() throws Exception
			LOG.error("Error while submitting requests", e);
		}
	}
		
	protected void submitWrapped(int n, TestType type) throws Exception {
		_syncfin = false;
		_totalOps = _zkBenchmark.getCurrentTotalOps();
		byte data[];

		for (int i = 0; i < _totalOps.get(); i++) {
			double submitTime = ((double)System.nanoTime() - _zkBenchmark.getStartTime())/1000000000.0;

			switch (type) {
				case READ:
					_client.getData().forPath(_path);
					break;

				case SETSINGLE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().forPath(_path, data);
					break;

				case SETMULTI:
					try {
						data = new String(_zkBenchmark.getData() + i).getBytes();
						_client.setData().forPath(_path + "/" + (_count % _highestN), data);
					} catch (NoNodeException e) {
						LOG.warn("No such node when setting data to mutiple nodes. " +
					             "_path = " + _path + ", _count = " + _count +
					             ", _highestN = " + _highestN, e);
					}
					break;

				case CREATE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.create().forPath(_path + "/" + _count, data);
					_highestN++;
					break;

				case DELETE:
					try {
						_client.delete().forPath(_path + "/" + _count);
					} catch (NoNodeException e) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("No such node (" + _path + "/" + _count +
									") when deleting nodes", e);
						}
					}
			}

			recordElapsedInterval(new Double(submitTime));
			_count++;
			_zkBenchmark.incrementFinished();

			if (_syncfin)
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
	protected void resubmit(int n) {
		_totalOps.getAndAdd(n);
	}
}
