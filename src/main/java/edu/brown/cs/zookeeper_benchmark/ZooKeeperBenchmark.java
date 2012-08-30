package edu.brown.cs.zookeeper_benchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

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
	
	enum TestType {
		READ, SETSINGLE, SETMULTI, CREATE, DELETE, CLEANING, UNDEFINED
	}

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperBenchmark.class);

	
	public ZooKeeperBenchmark(String[] hosts, int interval, int ops, int lowerbound, boolean sync) throws IOException {
		/*
		 * ops here represents the number of total number of ops submitted to server
		 * say 10000, then if it falls below 2000, submit another 8000 to reach 10000
		 * */
		_totalOps = ops;
		_lowerbound = lowerbound;
		_clients = new BenchmarkClient[hosts.length];
		_interval = interval;
		_running = new HashMap<Integer,Thread>();
		_deadline = 0;
		_barrier = new CyclicBarrier(_clients.length+1);
	
		_data = "";

		for (int i = 0; i < 20; i++) { // 100 bytes of important data
			_data += "!!!!!";
		}

		int avgOps = ops / hosts.length;

		for (int i = 0; i < hosts.length; i++) {
			if (sync) {
				_clients[i] = new SyncBenchmarkClient(this, hosts[i], "/zkTest", avgOps, i);
			} else {
				_clients[i] = new AsyncBenchmarkClient(this, hosts[i], "/zkTest", avgOps, i);
			}
		}
	}
	
	public void runBenchmark(int totaltime) {
		_deadline = totaltime / _interval;

		/* Read requests are done by zookeeper extremely
		 * quickly compared with write requests. If the time
		 * interval and threshold are not chosen appropriately,
		 * it could happen that when the timer awakes, all requests
		 * have already been finished. In this case, the output
		 * of read test doesn't reflect the actual rate of
		 * read requests. */
		doTest(TestType.READ);

		doTest(TestType.READ); // Do twice to allow for warm-up

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

		System.err.println("tests done. now cleaning-up.");

		for (int i = 0; i < _clients.length; i++) {
			_clients[i].setTest(TestType.CLEANING);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}

		synchronized (_running) {
			try {
				_running.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.err.println("all finished");
	}
	
	/* This is where each individual test starts */
	
	public void doTest(TestType test) {
		_currentTest = test;
		_finishedTotal = new AtomicInteger(0);
		_lastfinished = 0;
		_currentTotalOps = new AtomicInteger(_totalOps);

		try {
			_rateFile = new BufferedWriter(new FileWriter(new File(test+".dat")));
		} catch(IOException e) {
			e.printStackTrace();
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
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
		
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new ResubmitTimer() , _interval, _interval);

		// Wait for the test to finish

		synchronized (_running) {
			try {
				_running.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
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
			e.printStackTrace();
		}

		double time = getTime();
		System.err.println(test + " finished, time elapsed(sec):" + time +
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
	
	/*
	 * args[0] is the interval
	 * args[1] is the total number of requests, say 16000 requests are submitted, and
	 * whenever it is below 4000, submit another 16000 - 4000 requests, here args[1] = 16000
	 * args[2] is the threshold, it is 4000 in above example
	 * args[3] is how much time you want to run the test, in millisecond
	 * args[4] is syn or async test, 0 for async, non-0 for sync
	 */
	public static void main(String[] args) {

		if(args.length != 5){
			System.out.println("wrong parameters");
		}
		String[] hosts = new String[5];
		hosts[0] = "host1.pane.cs.brown.edu:2181";
		hosts[1] = "host2.pane.cs.brown.edu:2181";
		hosts[2] = "host3.pane.cs.brown.edu:2181";
		hosts[3] = "host4.pane.cs.brown.edu:2181";
		hosts[4] = "host5.pane.cs.brown.edu:2181";
/*		hosts[0] = "euc03.cs.brown.edu:2181";
		hosts[1] = "euc04.cs.brown.edu:2181";
		hosts[2] = "euc05.cs.brown.edu:2181";
		hosts[3] = "euc06.cs.brown.edu:2181";
		hosts[4] = "euc07.cs.brown.edu:2181"; */
		
		int interval = Integer.parseInt(args[0]);
		int totalnumber = Integer.parseInt(args[1]);
		int threshold = Integer.parseInt(args[2]);
		int time = Integer.parseInt(args[3]);
		boolean sync = Integer.parseInt(args[4]) == 0 ? false : true;
		/*String[] hosts = new String[1];
		hosts[0] = "localhost:2181";*/
		System.err.println(interval+"  "+totalnumber+" "+threshold+" "+time+" "+sync);

		try {
			ZooKeeperBenchmark benchmark = new ZooKeeperBenchmark(hosts, interval, totalnumber,
					threshold, sync);
			benchmark.runBenchmark(time);
		} catch (IOException e) {
			System.err.println("Failed to start ZooKeeper benchmark.");
			e.printStackTrace();
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
					e.printStackTrace();
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
