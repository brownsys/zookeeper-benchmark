package edu.brown.cs.zkbenchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

public class ZooKeeperBenchmark {
	private int _totalOps; // total operations requested by user
	private AtomicInteger _currentTotalOps; // possibly increased # of ops so test last for requested time
	private int _lowerbound;
	private BenchmarkClient[] _clients;
	private int _interval;
	private HashMap<Integer, Thread> _running;
	private AtomicInteger _finishedTotal;
	private int _lastfinished;
	private int _deadline; // in units of "_interval"
	private long _totalTimeSeconds;
	private long _lastCpuTime;
	private long _currentCpuTime;
	private long _startCpuTime;
	private int _dataSizeInBytes;
	private TestType _currentTest;	
	private String _data;
	private BufferedWriter _rateFile;
	private CyclicBarrier _barrier;
	private Boolean _finished;
	
	enum TestType {
		READ, SETSINGLE, SETMULTI, CREATE, DELETE, CLEANING, UNDEFINED
	}

	private static final Logger LOG = Logger.getLogger(ZooKeeperBenchmark.class);
	
	public ZooKeeperBenchmark(Configuration conf) throws IOException {
		LinkedList<String> serverList = new LinkedList<String>();
		Iterator<String> serverNames = conf.getKeys("server");

		while (serverNames.hasNext()) {
			String serverName = serverNames.next();
			String address = conf.getString(serverName);
			serverList.add(address);
		}
		
		if (serverList.size() == 0) {
			throw new IllegalArgumentException("ZooKeeper server addresses required");
		}
		
		_interval = conf.getInt("interval");
		_totalOps = conf.getInt("totalOperations");
		_lowerbound = conf.getInt("lowerbound");
		int totaltime = conf.getInt("totalTime");
		_totalTimeSeconds = Math.round((double) totaltime / 1000.0);
		boolean sync = conf.getBoolean("sync");
		_dataSizeInBytes = conf.getInt("dataSize", 100);
		_running = new HashMap<Integer,Thread>();
		_clients = new BenchmarkClient[serverList.size()];
		_barrier = new CyclicBarrier(_clients.length+1);
		_deadline = totaltime / _interval;
		
		LOG.info("benchmark set with: interval: " + _interval + " total number: " + _totalOps +
				" threshold: " + _lowerbound + " time: " + totaltime + " sync: " + (sync?"SYNC":"ASYNC") + " data size: " + _dataSizeInBytes + " bytes");

		_data = genZNodeData(_dataSizeInBytes);

		int avgOps = _totalOps / serverList.size();

		for (int i = 0; i < serverList.size(); i++) {
			if (sync) {
				_clients[i] = new SyncBenchmarkClient(this, serverList.get(i), "/zkTest", avgOps, i);
			} else {
				_clients[i] = new AsyncBenchmarkClient(this, serverList.get(i), "/zkTest", avgOps, i);
			}
		}
		
	}

	private String genZNodeData(int len){
		StringBuilder sb = new StringBuilder(len);
		for(int i = 0; i < len; i++) {
			sb.append("!");
		}
		return sb.toString();
	}
	
	public void runBenchmark() {

		/* Read requests are done by zookeeper extremely
		 * quickly compared with write requests. If the time
		 * interval and threshold are not chosen appropriately,
		 * it could happen that when the timer awakes, all requests
		 * have already been finished. In this case, the output
		 * of read test doesn't reflect the actual rate of
		 * read requests. */
		doTest(TestType.READ, "warm-up");

		doTest(TestType.READ, "znode read"); // Do twice to allow for warm-up

		doTest(TestType.SETSINGLE, "repeated single-znode write");

		doTest(TestType.CREATE, "znode create");

		doTest(TestType.SETMULTI, "different znode write");

		/* In the test, node creation and deletion tests are
		 * done by creating a lot of nodes at first and then
		 * deleting them. Since both of these two tests run
		 * for a certain time, there is no guarantee that which
		 * requests is more than the other. If there are more
		 * delete requests than create requests, the extra delete
		 * requests would end up not actually deleting anything.
		 * Though these requests are sent and processed by
		 * zookeeper server anyway, this could still be an issue.*/
		doTest(TestType.DELETE, "znode delete");

		LOG.info("Tests completed, now cleaning-up");

		for (int i = 0; i < _clients.length; i++) {
			_clients[i].setTest(TestType.CLEANING);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}

		while (!_finished) {
			synchronized (_running) {
				try {
					_running.wait();
				} catch (InterruptedException e) {
					LOG.warn("Benchmark main thread was interrupted while waiting", e);
				}
			}
		}

		LOG.info("All tests are complete");
	}
	
	/* This is where each individual test starts */
	
	public void doTest(TestType test, String description) {
		_currentTest = test;
		_finishedTotal = new AtomicInteger(0);
		_lastfinished = 0;
		_currentTotalOps = new AtomicInteger(_totalOps);
		_finished = false;

		System.out.print("Running " + description + " benchmark for " + _totalTimeSeconds + " seconds... ");

		try {
			_rateFile = new BufferedWriter(new FileWriter(new File(test+".dat")));
		} catch (IOException e) {
			LOG.error("Unable to create output file", e);
		}
		
		_startCpuTime = System.nanoTime();
		_lastCpuTime = _startCpuTime;


		// Start the testing clients!
		
		for (int i = 0; i < _clients.length; i++) {
			_clients[i].setTest(test);
			Thread tmp = new Thread(_clients[i]);			
			_running.put(new Integer(i), tmp);
			tmp.start();
		}

		// Wait for clients to connect to their assigned server, and
		// start timer which ensures we have outstanding requests.
		
		try {
			_barrier.await();
		} catch (BrokenBarrierException e) {
			LOG.warn("Some other client was interrupted; Benchmark main thread is out of sync", e);
		} catch (InterruptedException e) {
			LOG.warn("Benchmark main thread was interrupted while waiting on barrier", e);
		}		
		
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new ResubmitTimer() , _interval, _interval);

		// Wait for the test to finish

		while (!_finished) {
			synchronized (_running) {
				try {
					_running.wait();
				} catch (InterruptedException e) {
					LOG.warn("Benchmark main thread was interrupted while waiting", e);
				}
			}
		}

		// Test is finished

		_currentTest = TestType.UNDEFINED;
		timer.cancel();

		try {
			if (_rateFile != null) {
				_rateFile.close();
			}
		} catch (IOException e) {
			LOG.warn("Error while closing output file", e);
		}

		double time = getTime();
		LOG.info(test + " finished, time elapsed (sec): " + time +
				" operations: " + _finishedTotal.get() + " avg rate: " +
				_finishedTotal.get()/time);

		System.out.println("done");
	}

	/* return the max time consumed by each thread */
	double getTime() {
		double ret = 0;
	
		for (int i = 0; i < _clients.length; i++) {
			if (ret < _clients[i].getTimeCount())
				ret = _clients[i].getTimeCount();
		}

		return (ret * _interval)/1000.0;
	}

	// TODO(adf): currently unused. should we keep it?
	int getTotalOps() {
		/* return the total number of reqs done by all threads */
		int ret = 0;
		for (int i = 0; i < _clients.length; i++) {
			ret += _clients[i].getOpsCount();
		}
		return ret;
	}

	TestType getCurrentTest() {
		return _currentTest;
	}

	void incrementFinished() {
		_finishedTotal.incrementAndGet();
	}

	CyclicBarrier getBarrier() {
		return _barrier;
	}

	String getData() {
		return _data;
	}

	int getDeadline() {
		return _deadline;
	}

	AtomicInteger getCurrentTotalOps() {
		return _currentTotalOps;
	}

	int getInterval() {
		return _interval;
	}

	long getStartTime() {
		return _startCpuTime;
	}
	
	void notifyFinished(int id) {
		synchronized (_running) {
			_running.remove(new Integer(id));
			if (_running.size() == 0) {
				_finished = true;
				_running.notify();
			}
		}
	}
	
	private static PropertiesConfiguration initConfiguration(String[] args) {
		OptionSet options = null;
		OptionParser parser = new OptionParser();
		PropertiesConfiguration conf = null;

		// Setup the option parser
		parser.accepts("help", "print this help statement");
		parser.accepts("conf", "configuration file (required)").
			withRequiredArg().ofType(String.class).required();
		parser.accepts("interval", "interval between rate measurements").
			withRequiredArg().ofType(Integer.class);
		parser.accepts("ops", "total number of operations").
			withRequiredArg().ofType(Integer.class);
		parser.accepts("lbound",
			"lowerbound for the number of operations").
			withRequiredArg().ofType(Integer.class);
		parser.accepts("time", "time tests will run for (milliseconds)").
			withRequiredArg().ofType(Integer.class);
		parser.accepts("sync", "sync or async test").
			withRequiredArg().ofType(Boolean.class);

		// Parse and gather the arguments
		try {
			options = parser.parse(args);
		} catch (OptionException e) {
			System.out.println("\nError parsing arguments: " + e.getMessage() + "\n");
			try {
				parser.printHelpOn(System.out);
			} catch (IOException e2) {
				LOG.error("Exception while printing help message", e2);
			}
			System.exit(-1);
		}

		Integer interval = (Integer) options.valueOf("interval");
		Integer totOps = (Integer) options.valueOf("ops");
		Integer lowerbound = (Integer) options.valueOf("lbound");
		Integer time = (Integer) options.valueOf("time");
		Boolean sync = (Boolean) options.valueOf("sync");

		// Load and parse the configuration file
		String configFile = (String) options.valueOf("conf");
		LOG.info("Loading benchmark from configuration file: " + configFile);

		try {
			conf = new PropertiesConfiguration(configFile);
		} catch (ConfigurationException e) {
			LOG.error("Failed to read configuration file: " + configFile, e);
			System.exit(-2);
		}

		// If there are options from command line, override the conf
		if (interval != null)
			conf.setProperty("interval", interval);
		if (totOps != null)
			conf.setProperty("totalOperations", totOps);
		if (lowerbound != null)
			conf.setProperty("lowerbound", lowerbound);
		if (time != null)
			conf.setProperty("totalTime", time);
		if (sync != null)
			conf.setProperty("sync", sync);

		return conf;
	}

	public static void main(String[] args) {

		// Parse command line and configuration file
		PropertiesConfiguration conf = initConfiguration(args);

		// Helpful info for users of our default log4j configuration
		Appender a = Logger.getRootLogger().getAppender("file");
		if (a != null && a instanceof FileAppender) {
			FileAppender fa = (FileAppender) a;
			System.out.println("Detailed logs going to: " + fa.getFile());
		}

		// Run the benchmark
		try {
			ZooKeeperBenchmark benchmark = new ZooKeeperBenchmark(conf);
			benchmark.runBenchmark();
		} catch (IOException e) {
			LOG.error("Failed to start ZooKeeper benchmark", e);
		}

		System.exit(0);
	}

	class ResubmitTimer extends TimerTask {
		@Override
		public void run() {
			if (_currentTest == TestType.UNDEFINED) {
				return;
			}

			int finished = _finishedTotal.get();
			if (finished == 0) {
				return;
			}

			_currentCpuTime = System.nanoTime();

			if (_rateFile != null) {
				try {
					if (finished - _lastfinished > 0) {
						// Record the time elapsed and current rate
						String msg = ((double)(_currentCpuTime - _startCpuTime)/1000000000.0) + " " +
								((double)(finished - _lastfinished) /
										((double)(_currentCpuTime - _lastCpuTime) / 1000000000.0));
						_rateFile.write(msg+"\n");
					}
				} catch (IOException e) {
					LOG.error("Error when writing to output file", e);
				}
			}

			_lastCpuTime = _currentCpuTime;
			_lastfinished = finished;

			int numRemaining = _currentTotalOps.get() - finished;

			if (numRemaining <= _lowerbound) {
				int incr = _totalOps - numRemaining;

				_currentTotalOps.getAndAdd(incr);
				int avg = incr / _clients.length;

				for (int i = 0; i < _clients.length; i++) {
					_clients[i].resubmit(avg);
				}
			}
		}
	}
}
