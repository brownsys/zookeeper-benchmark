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

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;

import edu.brown.cs.zookeeper_benchmark.ZooKeeperBenchmark.TestType;

public abstract class BenchmarkClient implements Runnable{
	protected ZooKeeperBenchmark _curatorTest;
	protected String _host;//the host this client is connecting to
	protected CuratorFramework _client;//the actual client
	protected TestType _type;//current test
	protected int _attempts;
	protected String _path;
	protected int _id;
	protected int count;
	protected int countTime;
	protected Timer _timer;
	
	protected int _highestN;
	protected int _highestDeleted;
	
	protected BufferedWriter _recorder;
	
	int getTimeCount() {
		return countTime;
	}
	
	int getOpsCount(){
		return count;
	}
	
	ZooKeeperBenchmark getBenchmark() {
		return _curatorTest;
	}
	
	void setStat(TestType type) {
		_type = type;
	}
	
	void zkAdminCommand(String cmd) {
		String host = _host.split(":")[0];
		Socket socket = null;
		OutputStream os = null;
		InputStream is = null;	
		
		try {
			socket = new Socket(host, 2181);
			os = socket.getOutputStream();
			is = socket.getInputStream();
			os.write(cmd.getBytes());
			os.flush();
			byte[] b = new byte[1000];
			int len = is.read(b);

			while (len >= 0) {
				System.err.println(_id+" " + cmd + " command:\n" + new String(b, 0, len));
				len = is.read(b);
			}
			
			System.err.println(_id+" " + cmd + " command: done.");
			is.close();
			os.close();
			socket.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	void doClean() throws Exception {
		List<String> children;
		do {
			children = _client.getChildren().forPath(_path);
			for(String child : children) {
				_client.delete().inBackground().forPath(_path+"/"+child);
			}
			Thread.sleep(2000);
		} while(children.size()!=0);
	}
	
	class FinishTimer extends TimerTask {
		@Override
		public void run() {
			//this can be used to measure rate of each thread
			//at this moment, it is not necessary
			countTime++;

			if(countTime == BenchmarkClient.this._curatorTest.getDeadline()) {	
				this.cancel();
				finish();
			}
		}
	}
	
	@Override
	public void run() {
		if (!_client.isStarted())
			_client.start();		
		
		if (_type == TestType.CLEANING) {
			try {
				doClean();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			synchronized(_curatorTest.getThreadMap()) {
				_curatorTest.getThreadMap().remove(new Integer(_id));
				if(_curatorTest.getThreadMap().size() == 0)
					_curatorTest.getThreadMap().notify();
			}
			return;
		}
		
		zkAdminCommand("srst");
		
		try {
			_curatorTest.getBarrier().await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		} catch (BrokenBarrierException e1) {
			e1.printStackTrace();
		}
		
		count = 0;
		countTime = 0;	
		
		try {
			Stat stat = _client.checkExists().forPath(_path);
			if(stat == null) {
				_client.create().forPath(_path, _curatorTest.getData().getBytes());
			}

			//create the timer
			int interval =  _curatorTest.getInterval();
			_timer.scheduleAtFixedRate(new FinishTimer(), interval, interval);
			
			try {
				_recorder = new BufferedWriter(new FileWriter(new File(_id + 
						"-" + _type + "_timings.dat")));
			} catch (IOException e) {
				e.printStackTrace();
			}
				
			submit(_attempts, _type);

		} catch(Exception e) {
			e.printStackTrace();
		}
		
		zkAdminCommand("stat");

		try {
			_recorder.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.err.println(_id + "-i'm done, reqs:" + count);
		synchronized(_curatorTest.getThreadMap()) {
			_curatorTest.getThreadMap().remove(new Integer(_id));
			if (_curatorTest.getThreadMap().size() == 0)
				_curatorTest.getThreadMap().notify();
		}	
		
	}


	void recordEvent(CuratorEvent event) throws IOException {
		Double oldctx = (Double)event.getContext();
		recordTimes(oldctx);
	}
	
	
	void recordTimes(Double startTime) throws IOException {
		double endtime = ((double)System.nanoTime() - _curatorTest.getStartTime())/1000000000.0;			
		_recorder.write(startTime.toString() + " " + Double.toString(endtime) + "\n");
	}
	
	abstract protected void submit(int n, TestType type) throws Exception;
	/**
	 * for synchronous requests, to submit more requests only needs to increase the total 
	 * number of requests, here n can be an arbitrary number
	 * for asynchronous requests, to submit more requests means that the client will do
	 * both submit and wait
	 * @param n
	 * @throws Exception
	 */
	abstract protected void resubmit(int n) throws Exception;
	
	abstract protected void finish();
}
