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

	int attempts;

	int _totalOps;
	int _lowerbound;
	BenchmarkClient[] _clients;
	String[] _hosts;
	int _interval;
	HashMap<Integer, Thread> _running;
	AtomicInteger _finishedTotal;
	int _oldTotal;
	int _timeCounter;
	int _deadline;
	AtomicInteger _curtotalOps;
	long _lastCpuTime;
	long _currCpuTime;
	long _startCpuTime;
	
	
	int _increment;
	testStat _currentTest;
	
	String _data;
	boolean _sync;
	
	BufferedWriter _bw;
	
	CyclicBarrier _barrier;
	
	enum testStat{
		READ, SETSINGLE, SETMUTI, CREATE, DELETE, CLEANING, UNDEFINED
	}

	void recordTimes(Double firstTime, BufferedWriter bw) throws IOException{
		double newtime = ((double)System.nanoTime() - _startCpuTime)/1000000000.0;
		String newTimeStr = Double.toString(newtime);				
		bw.write(firstTime.toString()+" "+newTimeStr+"\n");
	}

	void recordEvent(CuratorEvent arg1, BufferedWriter bw) throws IOException{
		Double oldctx = (Double)arg1.getContext();
		recordTimes(oldctx, bw);
	}
	
	double getTime(){
		/*return the max time consumed by each thread*/
		double ret = 0;
		for(int i = 0;i<_clients.length;i++){
			if(ret < _clients[i].getTimeCount())
				ret = _clients[i].getTimeCount();
		}
		return (ret * _interval)/1000.0;
	}
	
	int getTotalOps(){
		/*return the total number of reqs done by all threads*/
		int ret = 0;
		for(int i = 0;i<_clients.length;i++){
			ret += _clients[i].getOpsCount();
		}
		return ret;
	}
	
	
	public void doTest(testStat stat) throws InterruptedException{
		_finishedTotal = new AtomicInteger(0);
		_oldTotal = 0;
		_curtotalOps = new AtomicInteger(_totalOps);
		try{
			_bw = new BufferedWriter(new FileWriter(new File(stat+".dat")));
		}catch(IOException e){
			e.printStackTrace();
		}
		
		_startCpuTime = System.nanoTime();
		_lastCpuTime = _startCpuTime;
		
		for(int i = 0;i<_hosts.length;i++){
			_clients[i].setStat(stat);
			Thread tmp = new Thread(_clients[i]);			
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		
		_currentTest = stat;
		try {
			_barrier.await();
		} catch (BrokenBarrierException e2) {
			e2.printStackTrace();
		}		
		
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				_timeCounter ++;
				if(_currentTest != testStat.UNDEFINED){
					int finished = _finishedTotal.get();
					if(finished == 0){
						//this means even the first batch of operations haven't
						return;
					}
					/*if(_startCpuTime == 0){
						_startCpuTime = System.nanoTime();
						_lastCpuTime = _startCpuTime;
						_currCpuTime = _startCpuTime;						
					}*/
					//System.err.println("increment:"+(finished - _oldTotal));
					_currCpuTime = System.nanoTime();
					
					
					String msg = ((double)(_currCpuTime - _startCpuTime)/1000000000.0)+" "
					+((double)(finished - _oldTotal)/((double)(_currCpuTime - _lastCpuTime)/1000000000.0));
					// System.out.println(msg);
					_lastCpuTime = _currCpuTime;
					
					if(_bw != null){
						try {
							if (finished - _oldTotal > 0) {
								_bw.write(msg+"\n");
							}
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
					_oldTotal = finished;
					if(_curtotalOps.get() - finished <= _lowerbound){
						_increment = _totalOps - (_curtotalOps.get() - finished);
						try{
							int avg = _increment / _clients.length;
							if(!_sync){
/*								for(int i = 0;i<_clients.length;i++){
									_clients[i].submitAsync(avg, _currentTest);
								}
								_curtotalOps.getAndAdd(_increment); */
							}else{
								_curtotalOps.getAndAdd(10000);
							}						
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}
			}			
		}, _interval, _interval);

		synchronized(_running){
			_running.wait();
		}
		_currentTest = testStat.UNDEFINED;

		timer.cancel();
		try {
			_bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		double time = getTime();
		System.err.println(stat+" finished, time elapsed(sec):"+time
				+" operations:"+_finishedTotal.get()+" avg rate:"+_finishedTotal.get()/time);
	}
	
	public void launch(int totaltime, boolean sync) throws InterruptedException{		
		_timeCounter = 0;
		_finishedTotal = new AtomicInteger(0);
		_oldTotal = 0;
		_deadline = totaltime / _interval;
		_sync = sync;
		
		/*this is where all tests start*/
		
		/*Read requests done done by zookeeper extremely 
		 * fast compared with write requests. If the time
		 * interval and threshold are not chosen appropriately, 
		 * it could happen that when the timer awakes, all requests 
		 * have already been finished. In this case, the output 
		 * of read test doesn't reflect the actual rate of 
		 * read requests. */
		doTest(testStat.READ);
		
		doTest(testStat.READ);
		
		doTest(testStat.SETSINGLE);
		
		doTest(testStat.CREATE);
		
		doTest(testStat.SETMUTI);
		
		/*In the test, node creation and deletion tests are 
		 * done by creating a lot of nodes at first and then 
		 * deleting them. Since both of these two tests run 
		 * for a certain time, there is no guarantee that which 
		 * requests is more than the other. If there are more 
		 * delete requests than create requests, the extra delete 
		 * requests would end up not actually deleting anything. 
		 * Though these requests are sent and processed by 
		 * zookeeper server anyway, this could still be an issue.*/
		doTest(testStat.DELETE);
		
		System.err.println("tests done cleaning");

		for(int i = 0;i<_hosts.length;i++){			
			_clients[i].setStat(testStat.CLEANING);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}
		synchronized(_running){
			_running.wait();
		}
		System.err.println("all finished");
	}	
	
	public ZooKeeperBenchmark(String[] hs, int interval, int ops, int lowerbound) throws IOException{
		/*
		 * ops here represents the number of total number of ops submitted to server
		 * say 10000, then if it falls below 2000, submit another 8000 to reach 10000
		 * */
		_totalOps = ops; 
		_lowerbound = lowerbound;
		_hosts = hs;
		_clients = new BenchmarkClient[hs.length];
		_interval = interval;
		int avgOps = ops/hs.length;
		for(int i = 0;i<hs.length;i++){
			_clients[i] = new BenchmarkClient(this, hs[i], "/zkTest", avgOps, i);
			
		}
		_running = new HashMap<Integer,Thread>();
		_deadline = 0;
		_increment = ops - lowerbound;
		
		_data = "!!!!!";
		for(int i = 0;i<19;i++){
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
	public static void main(String[] args) throws Exception{
		
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
				interval, totalnumber, threshold);
		test.launch(time, sync);
		System.exit(0);
		
	}
}
