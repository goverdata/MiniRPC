package com.github.dtf;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.github.dtf.task.scheduler.TaskScheduler;

public class JobTracker implements TaskTrackerManager {
	private TaskScheduler taskScheduler;
	private int totalSubmissions;
	// All the known jobs. (jobid->JobInProgress)
	Map<JobID, JobInProgress> jobs = new TreeMap<JobID, JobInProgress>();

	/*
	 * public Collection<TaskTrackerStatus> taskTrackers() { // TODO
	 * Auto-generated method stub return null; }
	 */

	public int getNumberOfUniqueHosts() {
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * public ClusterStatus getClusterStatus() { // TODO Auto-generated method
	 * stub return null; }
	 */

	/*
	 * public QueueManager getQueueManager() { // TODO Auto-generated method
	 * stub return null; }
	 */

	public int getNextHeartbeatInterval() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void killJob(JobID jobid) throws IOException {
		// TODO Auto-generated method stub

	}

	public JobInProgress getJob(JobID jobid) {
		// TODO Auto-generated method stub
		return null;
	}

	public void initJob(JobInProgress job) {
		// TODO Auto-generated method stub

	}

	public void failJob(JobInProgress job) {
		// TODO Auto-generated method stub

	}

	private synchronized JobStatus addJob(JobID jobId, JobInProgress job) {
		totalSubmissions++;
		synchronized (jobs) {
			synchronized (taskScheduler) {
				jobs.put(job.getProfile().getJobID(), job);
				taskScheduler.addJob(jobId, job);
			}
		}
		myInstrumentation.submitJob(job.getJobConf(), jobId);
		return job.getStatus();
	}
}
