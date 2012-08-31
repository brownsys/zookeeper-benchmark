package edu.brown.cs.zookeeper_benchmark;

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

import joptsimple.OptionParser;
import joptsimple.OptionSet;


import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZooKeeperBenchmark {
	private int _totalOps; // total operations requested by user
	private AtomicInteger _currentTotalOps; // possibly increased # of ops so test last for requested time
	private int _lowerbound;
	private BenchmarkClient[] _clients;
	private int _interval;
	private HashMap<Integer, Thread> _running;
	private AtomicInteger _finishedTotal;
	private int _lastfinished;
	private int _deadline;
	private long _lastCpuTime;
	private long _currentCpuTime;
	private long _startCpuTime;
	private TestType _currentTest;	
	private String _data;
	private BufferedWriter _rateFile;
	private CyclicBarrier _barrier;
	private Boolean _finish;
	
	enum TestType {
		READ, SETSINGLE, SETMULTI, CREATE, DELETE, CLEANING, UNDEFINED
	}

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperBenchmark.class);
	
	public ZooKeeperBenchmark(Configuration conf) throws IOException {
		
		LinkedList<String> serverList = new LinkedList<String>();
		Iterator<String> serverNames = conf.getKeys("server");
		
		while(serverNames.hasNext()) {
			String serverName = serverNames.next();
			String address = conf.getString(serverName);
			serverList.add(address);
		}
		
		if(serverList.size() == 0) {
			throw new IllegalArgumentException("ZooKeeper server addresses required");
		}
		
		_interval = conf.getInt("interval");
		_totalOps = conf.getInt("totalOperation");
		_lowerbound = conf.getInt("lowerbound");
		int totaltime = conf.getInt("totalTime");
		boolean sync = conf.getBoolean("sync");
		
		_running = new HashMap<Integer,Thread>();		
		_clients = new BenchmarkClient[serverList.size()];
		_barrier = new CyclicBarrier(_clients.length+1);
		_deadline = totaltime / _interval;
		
		LOG.info("benchmark set with: interval:" + _interval + " total number:" + _totalOps +
				" threshold:" + _lowerbound + " time:" + totaltime + " sync:" + (sync?"SYNC":"ASYNC"));

		
		_data = "";

		for (int i = 0; i < 20; i++) { // 100 bytes of important data
			_data += "!!!!!";
		}

		int avgOps = _totalOps / serverList.size();

		for (int i = 0; i < serverList.size(); i++) {
			if (sync) {
				_clients[i] = new SyncBenchmarkClient(this, serverList.get(i), "/zkTest", avgOps, i);
			} else {
				_clients[i] = new AsyncBenchmarkClient(this, serverList.get(i), "/zkTest", avgOps, i);
			}
		}
		
	}
	
	public void runBenchmark() {

		/* Read requests are done by zookeeper extremely
		 * quickly compared with write requests. If the time
		 * interval and threshold are not chosen appropriately,
		 * it could happen that when the timer awakes, all requests
		 * have already been finished. In this case, the output
		 * of read test doesn't reflect the actual rate of
		 * read requests. */
		//doTest(TestType.READ);

		//doTest(TestType.READ); // Do twice to allow for warm-up

		doTest(TestType.SETSINGLE);

		doTest(TestType.CREATE);

		doTest(TestType.SETMULTI);

		/* In the test, node creation and deletion tests are
		 * done by creating a lot of nodes at first and then
		 * deleting them. Since both of these two tests run
		 * for a certain time, there is no guarantee that which
		 * requests is more than the other. If there are more
		 * delete requests than create requests, the extra delete
		 * requests would end up not actually deleting anything.
		 * Though these requests are sent and processed by
		 * zookeeper server anyway, this could still be an issue.*/
		doTest(TestType.DELETE);

		LOG.info("Tests completed, now cleaning-up");

		for (int i = 0; i < _clients.length; i++) {
			_clients[i].setTest(TestType.CLEANING);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}

		while(!_finish) {
			synchronized (_running) {
				try {
					_running.wait();
				} catch (InterruptedException e) {
					LOG.warn("Benmark main thread is interruptted when waiting:" + e.getMessage());
				}
			}
		}

		LOG.info("All tests are completed");
	}
	
	/* This is where each individual test starts */
	
	public void doTest(TestType test) {
		_currentTest = test;
		_finishedTotal = new AtomicInteger(0);
		_lastfinished = 0;
		_currentTotalOps = new AtomicInteger(_totalOps);
		_finish = false;

		try {
			_rateFile = new BufferedWriter(new FileWriter(new File(test+".dat")));
		} catch(IOException e) {
			LOG.error("Error when creating output file:" + e.getMessage());
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
			LOG.warn("Some other client is interrupted, Benchmark main thread is out of sync:" + e.getMessage());
		} catch (InterruptedException e) {
			LOG.warn("Benchmark main thread is interrupted when waiting on barrier:" + e.getMessage());
		}		
		
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new ResubmitTimer() , _interval, _interval);

		// Wait for the test to finish

		while(!_finish) {
			synchronized (_running) {
				try {
					_running.wait();
				} catch (InterruptedException e) {
					LOG.warn("Benmark main thread is interruptted when waiting:" + e.getMessage());
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
			LOG.warn("Error when closing output file:" + e.getMessage());
		}

		double time = getTime();
		LOG.info(test + " finished, time elapsed(sec):" + time +
				" operations:" + _finishedTotal.get() + " avg rate:" +
				_finishedTotal.get()/time);
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

	HashMap<Integer, Thread> getThreadMap() {
		return _running;
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

	void testFinish() {
		_finish = true;
		_running.notify();
	}
	
	public static void main(String[] args) throws Exception {

		OptionParser parser = new OptionParser();
		parser.accepts("c", "configuration file (required)").
		withRequiredArg().ofType(String.class);
		parser.accepts("help", "print help statement");
		OptionSet options = parser.parse(args);

		if (options.has("help") || !options.has("c")) {
			parser.printHelpOn(System.out);
			System.exit(-1);
		}

		BasicConfigurator.configure();

		String configFile = (String) options.valueOf("c");
		PropertiesConfiguration conf = new PropertiesConfiguration(configFile);

		try {
			ZooKeeperBenchmark benchmark = new ZooKeeperBenchmark(conf);
			benchmark.runBenchmark();
		} catch (IOException e) {
			LOG.error("Failed to start ZooKeeper benchmark.");
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
					LOG.error("Error when writing to output file:" + e.getMessage());
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
