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
import com.netflix.curator.framework.api.CuratorEvent;

public class ZooKeeperBenchmark {

	private int _totalOps;
	private int _lowerbound;
	private BenchmarkClient[] _clients;
	private String[] _hosts;
	private int _interval;
	private HashMap<Integer, Thread> _running;
	private AtomicInteger _finishedTotal;
	private int _lastfinished;
	private int _timeCounter;
	private int _deadline;
	private AtomicInteger _currentTotalOps;
	private long _lastCpuTime;
	private long _currentCpuTime;
	private long _startCpuTime;
	private int _increment;
	private TestType _currentTest;	
	private String _data;
	private boolean _sync;
	
	private BufferedWriter _bw;
	
	private CyclicBarrier _barrier;
	
	enum TestType {
		READ, SETSINGLE, SETMUTI, CREATE, DELETE, CLEANING, UNDEFINED
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
	
	boolean isSync() {
		return _sync;
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
	
	double getTime() {
		/*return the max time consumed by each thread*/
		double ret = 0;
		for(int i = 0;i<_clients.length;i++) {
			if(ret < _clients[i].getTimeCount())
				ret = _clients[i].getTimeCount();
		}
		return (ret * _interval)/1000.0;
	}
	
	int getTotalOps() {
		/*return the total number of reqs done by all threads*/
		int ret = 0;
		for(int i = 0;i<_clients.length;i++) {
			ret += _clients[i].getOpsCount();
		}
		return ret;
	}
	
	class ResubmitTimer extends TimerTask {
		@Override
		public void run() {
			_timeCounter ++;
			if (_currentTest != TestType.UNDEFINED) {
				int finished = _finishedTotal.get();
				if (finished == 0) {
					return;
				}
				
				_currentCpuTime = System.nanoTime();					
				
				String msg = ((double)(_currentCpuTime - _startCpuTime)/1000000000.0) + " " + 
				((double)(finished - _lastfinished)/((double)(_currentCpuTime - _lastCpuTime)/1000000000.0));
				// System.out.println(msg);
				_lastCpuTime = _currentCpuTime;
				
				if (_bw != null) {
					try {
						if (finished - _lastfinished > 0) {
							_bw.write(msg+"\n");
						}
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
				
				_lastfinished = finished;
				if (_currentTotalOps.get() - finished <= _lowerbound) {
					_increment = _totalOps - (_currentTotalOps.get() - finished);
					try{							
						_currentTotalOps.getAndAdd(_increment);
						int avg = _increment / _clients.length;
						for (int i = 0;i<_clients.length;i++) {
							_clients[i].resubmit(avg);
						}				
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}			
		}		
	}
	
	public void doTest(TestType stat) throws InterruptedException {
		_finishedTotal = new AtomicInteger(0);
		_lastfinished = 0;
		_currentTotalOps = new AtomicInteger(_totalOps);
		try {
			_bw = new BufferedWriter(new FileWriter(new File(stat+".dat")));
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		_startCpuTime = System.nanoTime();
		_lastCpuTime = _startCpuTime;
		
		for(int i = 0;i<_hosts.length;i++) {
			_clients[i].setStat(stat);
			Thread tmp = new Thread(_clients[i]);			
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		
		_currentTest = stat;
		try {
			_barrier.await();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}		
		
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new ResubmitTimer() , _interval, _interval);

		synchronized(_running) {
			_running.wait();
		}
		_currentTest = TestType.UNDEFINED;

		timer.cancel();
		try {
			_bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		double time = getTime();
		System.err.println(stat + " finished, time elapsed(sec):" + time + 
				" operations:" + _finishedTotal.get() + " avg rate:" + 
				_finishedTotal.get()/time);
	}
	
	public void launch(int totaltime) throws InterruptedException {		
		_timeCounter = 0;
		_deadline = totaltime / _interval;
		
		/*this is where all tests start*/
		
		/*Read requests done done by zookeeper extremely 
		 * fast compared with write requests. If the time
		 * interval and threshold are not chosen appropriately, 
		 * it could happen that when the timer awakes, all requests 
		 * have already been finished. In this case, the output 
		 * of read test doesn't reflect the actual rate of 
		 * read requests. */
		doTest(TestType.READ);
		
		doTest(TestType.READ);
		
		doTest(TestType.SETSINGLE);
		
		doTest(TestType.CREATE);
		
		doTest(TestType.SETMUTI);
		
		/*In the test, node creation and deletion tests are 
		 * done by creating a lot of nodes at first and then 
		 * deleting them. Since both of these two tests run 
		 * for a certain time, there is no guarantee that which 
		 * requests is more than the other. If there are more 
		 * delete requests than create requests, the extra delete 
		 * requests would end up not actually deleting anything. 
		 * Though these requests are sent and processed by 
		 * zookeeper server anyway, this could still be an issue.*/
		doTest(TestType.DELETE);
		
		System.err.println("tests done cleaning");

		for(int i = 0;i<_hosts.length;i++) {			
			_clients[i].setStat(TestType.CLEANING);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running) {
			_running.wait();
		}
		System.err.println("all finished");
	}	
	
	public ZooKeeperBenchmark(String[] hs, int interval, int ops, int lowerbound, boolean sync) throws IOException {
		/*
		 * ops here represents the number of total number of ops submitted to server
		 * say 10000, then if it falls below 2000, submit another 8000 to reach 10000
		 * */
		_totalOps = ops; 
		_lowerbound = lowerbound;
		_hosts = hs;
		_clients = new BenchmarkClient[hs.length];
		_interval = interval;
		_sync = sync;
		int avgOps = ops/hs.length;
		for(int i = 0;i<hs.length;i++){
			if(_sync)
				_clients[i] = new SyncBenchmarkClient(this, hs[i], "/zkTest", avgOps, i);
			else
				_clients[i] = new AsyncBenchmarkClient(this, hs[i], "/zkTest", avgOps, i);			
		}
		_running = new HashMap<Integer,Thread>();
		_deadline = 0;
		_increment = ops - lowerbound;
		
		_data = "!!!!!";
		for(int i = 0;i<19;i++) {
			_data += "!!!!!";
		}
		_barrier = new CyclicBarrier(_clients.length+1);
		System.err.println(_barrier.getParties());
	}	
	
	/*
	 * args[0] is the interval
	 * args[1] is the total number of requests, say 16000 requests are submitted, and 
	 * whenever it is below 4000, submit another 16000 - 4000 requests, here args[1] = 16000
	 * args[2] is the threshold, it is 4000 in above example
	 * args[3] is how much time you want to run the test, in millisecond
	 * args[4] is syn or async test, 0 for async, non-0 for sync
	 */
	public static void main(String[] args) throws Exception {
		
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
		ZooKeeperBenchmark test = new ZooKeeperBenchmark(hosts, 
				interval, totalnumber, threshold, sync);
		test.launch(time);
		System.exit(0);
		
	}
}
