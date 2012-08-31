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

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.retry.RetryNTimes;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.TestType;

public abstract class BenchmarkClient implements Runnable {
	protected ZooKeeperBenchmark _zkBenchmark;
	protected String _host; // the host this client is connecting to
	protected CuratorFramework _client; // the actual client
	protected TestType _type; // current test
	protected int _attempts;
	protected String _path;
	protected int _id;
	protected int _count;
	protected int _countTime;
	protected Timer _timer;
	
	protected int _highestN;
	protected int _highestDeleted;
	
	protected BufferedWriter _latenciesFile;
	
	private static final Logger LOG = LoggerFactory.getLogger(BenchmarkClient.class);


	public BenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace,
			int attempts, int id) throws IOException {
		_zkBenchmark = zkBenchmark;
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
	public void run() {
		if (!_client.isStarted())
			_client.start();		
		
		if (_type == TestType.CLEANING) {
			doCleaning();
			return;
		}
		
		zkAdminCommand("srst"); // Reset ZK server's statistics
		
		// Wait for all clients to be ready

		try {
			_zkBenchmark.getBarrier().await();
		} catch (InterruptedException e) {
			LOG.warn("Client#" + _id + " is interrupted when waiting on barrier:" + e);
		} catch (BrokenBarrierException e) {
			LOG.warn("Some other client is interrupted, Client#" + _id + " is out of sync:" + e);
		}
		
		_count = 0;
		_countTime = 0;
		
		// Create a directory to work in

		try {
			Stat stat = _client.checkExists().forPath(_path);
			if (stat == null) {
				_client.create().forPath(_path, _zkBenchmark.getData().getBytes());
			}
		} catch (Exception e) {
			LOG.error("Error when creating working directory:" + e);
		}

		// Create a timer to check when we're finished. Schedule it to run
	    // periodically in case we want to record periodic statistics

		int interval = _zkBenchmark.getInterval();
		_timer.scheduleAtFixedRate(new FinishTimer(), interval, interval);

		try {
			_latenciesFile = new BufferedWriter(new FileWriter(new File(_id +
					"-" + _type + "_timings.dat")));
		} catch (IOException e) {
			LOG.error("Error when creating output file:" + e);
		}

		// Submit the requests!

		submit(_attempts, _type);

		// Test is complete. Print some stats and go home.

		zkAdminCommand("stat");


		try {
			if (_latenciesFile != null)
				_latenciesFile.close();				
		} catch (IOException e) {
			LOG.warn("Error when closing output file:" + e);
		}

		LOG.info("client#" + _id + " current test completed, completed " + _count + "requests:");

		_zkBenchmark.notifyFinished(_id);
		
	}

	class FinishTimer extends TimerTask {
		@Override
		public void run() {
			//this can be used to measure rate of each thread
			//at this moment, it is not necessary
			_countTime++;

			if (_countTime == _zkBenchmark.getDeadline()) {
				this.cancel();
				finish();
			}
		}
	}

	void doCleaning() {
		try {
			deleteChildren();
		} catch (Exception e) {
			LOG.error("Exception when deleting old znodes" + e.getMessage());
		}

		_zkBenchmark.notifyFinished(_id);
	}

	/* Delete all the child znodes created by this client */
	void deleteChildren() throws Exception {
		List<String> children;

		do {
			children = _client.getChildren().forPath(_path);
			for (String child : children) {
				_client.delete().inBackground().forPath(_path + "/" + child);
			}
			Thread.sleep(2000);
		} while (children.size() != 0);
	}


	void recordEvent(CuratorEvent event) {
		Double submitTime = (Double) event.getContext();
		recordElapsedInterval(submitTime);
	}


	void recordElapsedInterval(Double startTime) {
		double endtime = ((double)System.nanoTime() - _zkBenchmark.getStartTime())/1000000000.0;

		try {
			_latenciesFile.write(startTime.toString() + " " + Double.toString(endtime) + "\n");
		} catch (IOException e) {
			LOG.error("Exceptions when writing to file:" + e);
		}
	}

	/* Send a command directly to the ZooKeeper server */
	void zkAdminCommand(String cmd) {
		String host = _host.split(":")[0];
		Socket socket = null;
		OutputStream os = null;
		InputStream is = null;
		byte[] b = new byte[1000];

		try {
			socket = new Socket(host, 2181);
			os = socket.getOutputStream();
			is = socket.getInputStream();

			os.write(cmd.getBytes());
			os.flush();

			int len = is.read(b);
			while (len >= 0) {
				LOG.info("client#" + _id + " is sending " + cmd +
						" command:\n" + new String(b, 0, len));
				len = is.read(b);
			}

			is.close();
			os.close();
			socket.close();
		} catch (UnknownHostException e) {
			LOG.error("Error when contacting ZooKeeper server: unknown host:" + e);
		} catch (IOException e) {
			LOG.error("Error when contacting ZooKeeper server: ioexception:" + e);
		} 
	}

	int getTimeCount() {
		return _countTime;
	}
	
	int getOpsCount(){
		return _count;
	}
	
	ZooKeeperBenchmark getBenchmark() {
		return _zkBenchmark;
	}
	
	void setTest(TestType type) {
		_type = type;
	}

	abstract protected void submit(int n, TestType type);

	/**
	 * for synchronous requests, to submit more requests only needs to increase the total 
	 * number of requests, here n can be an arbitrary number
	 * for asynchronous requests, to submit more requests means that the client will do
	 * both submit and wait
	 * @param n
	 */
	abstract protected void resubmit(int n);
	
	abstract protected void finish();
}
