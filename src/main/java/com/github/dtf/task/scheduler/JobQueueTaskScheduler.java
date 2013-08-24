package com.github.dtf.task.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.dtf.JobID;
import com.github.dtf.JobInProgress;
import com.github.dtf.JobInProgressListener;
import com.github.dtf.job.JobStatus;
import com.github.dtf.task.Task;
import com.github.dtf.task.TaskTrackerStatus;

public class JobQueueTaskScheduler extends TaskScheduler {

	private static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
	public static final Log LOG = LogFactory.getLog(JobQueueTaskScheduler.class);

	protected JobQueueJobInProgressListener jobQueueJobInProgressListener;
	protected EagerTaskInitializationListener eagerTaskInitializationListener;
	private final List<JobInProgressListener> jobInProgressListeners = new CopyOnWriteArrayList<JobInProgressListener>();
	private float padFraction;

	
	public JobQueueTaskScheduler() {
		this.jobQueueJobInProgressListener = new JobQueueJobInProgressListener();
	}

	@Override
	public synchronized void start() throws IOException {
		super.start();
		addJobInProgressListener(jobQueueJobInProgressListener);
		eagerTaskInitializationListener
				.setTaskTrackerManager(taskTrackerManager);
		eagerTaskInitializationListener.start();
		addJobInProgressListener(eagerTaskInitializationListener);
	}

	@Override
	public synchronized void terminate() throws IOException {
		if (jobQueueJobInProgressListener != null) {
			removeJobInProgressListener(jobQueueJobInProgressListener);
		}
		if (eagerTaskInitializationListener != null) {
			removeJobInProgressListener(eagerTaskInitializationListener);
			eagerTaskInitializationListener.terminate();
		}
		super.terminate();
	}

	/*
	 * @Override public synchronized void setConf(Configuration conf) {
	 * super.setConf(conf); padFraction =
	 * conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 0.01f);
	 * this.eagerTaskInitializationListener = new
	 * EagerTaskInitializationListener(conf); }
	 */

	@Override
	public synchronized List<Task> assignTasks(TaskTrackerStatus taskTracker)
			throws IOException {

		ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
		final int numTaskTrackers = clusterStatus.getTaskTrackers();
		final int clusterCapacity = clusterStatus.getMaxTasks();
//		final int clusterMapCapacity = clusterStatus.getMaxMapTasks();
//		final int clusterReduceCapacity = clusterStatus.getMaxReduceTasks();

		Collection<JobInProgress> jobQueue = jobQueueJobInProgressListener.getJobQueue();

		//
		// Get map + reduce counts for the current tracker.
		//
//		final int trackerMapCapacity = taskTracker.getMaxMapTasks();
//		final int trackerReduceCapacity = taskTracker.getMaxReduceTasks();
//		final int trackerRunningMaps = taskTracker.countMapTasks();
//		final int trackerRunningReduces = taskTracker.countReduceTasks();
		final int trackerCapacity = taskTracker.getMaxTasks();
		final int trackerRunning = taskTracker.countTasks();

		// Assigned tasks
		List<Task> assignedTasks = new ArrayList<Task>();

		//
		// Compute (running + pending) map and reduce task numbers across pool
		//
		int remainingMapLoad = 0;
		synchronized (jobQueue) {
			for (JobInProgress job : jobQueue) {
				if (job.getStatus().getRunState() == JobStatus.RUNNING) {
					remainingMapLoad += (job.desiredTasks() - job.finishedTasks());
				}
			}
		}

		// Compute the 'load factor' for tasks
		double mapLoadFactor = 0.0;
		if (clusterCapacity > 0) {
			mapLoadFactor = (double) remainingMapLoad / clusterCapacity;
		}

		//
		// In the below steps, we allocate first map tasks (if appropriate),
		// and then reduce tasks if appropriate. We go through all jobs
		// in order of job arrival; jobs only get serviced if their
		// predecessors are serviced, too.
		//

		//
		// We assign tasks to the current taskTracker if the given machine
		// has a workload that's less than the maximum load of that kind of
		// task.
		// However, if the cluster is close to getting loaded i.e. we don't
		// have enough _padding_ for speculative executions etc., we only
		// schedule the "highest priority" task i.e. the task from the job
		// with the highest priority.
		//

		final int trackerCurrentMapCapacity = Math.min(
				(int) Math.ceil(mapLoadFactor * trackerCapacity),
				trackerCapacity);
		int availableMapSlots = trackerCurrentMapCapacity - trackerRunning;
		boolean exceededMapPadding = false;
		if (availableMapSlots > 0) {
			exceededMapPadding = exceededPadding(true, clusterStatus,
					trackerCapacity);
		}

		int numLocalMaps = 0;
		int numNonLocalMaps = 0;
		scheduleMaps: for (int i = 0; i < availableMapSlots; ++i) {
			synchronized (jobQueue) {
				for (JobInProgress job : jobQueue) {
					if (job.getStatus().getRunState() != JobStatus.RUNNING) {
						continue;
					}

					Task t = null;

					// Try to schedule a node-local or rack-local Map task
					t = job.obtainNewLocalTask(taskTracker, numTaskTrackers,
							taskTrackerManager.getNumberOfUniqueHosts());
					if (t != null) {
						assignedTasks.add(t);
						++numLocalMaps;

						// Don't assign map tasks to the hilt!
						// Leave some free slots in the cluster for future
						// task-failures,
						// speculative tasks etc. beyond the highest priority
						// job
						if (exceededMapPadding) {
							break scheduleMaps;
						}

						// Try all jobs again for the next Map task
						break;
					}

					// Try to schedule a node-local or rack-local Map task
					t = job.obtainNewNonLocalTask(taskTracker,
							numTaskTrackers,
							taskTrackerManager.getNumberOfUniqueHosts());

					if (t != null) {
						assignedTasks.add(t);
						++numNonLocalMaps;

						// We assign at most 1 off-switch or speculative task
						// This is to prevent TaskTrackers from stealing
						// local-tasks
						// from other TaskTrackers.
						break scheduleMaps;
					}
				}
			}
		}
		int assignedMaps = assignedTasks.size();

		if (LOG.isDebugEnabled()) {
			/*LOG.debug("Task assignments for " + taskTracker.getTrackerName()
					+ " --> " + "[" + mapLoadFactor + ", " + trackerMapCapacity
					+ ", " + trackerCurrentMapCapacity + ", "
					+ trackerRunningMaps + "] -> ["
					+ (trackerCurrentMapCapacity - trackerRunningMaps) + ", "
					+ assignedMaps + " (" + numLocalMaps + ", "
					+ numNonLocalMaps + ")] [" + reduceLoadFactor + ", "
					+ trackerReduceCapacity + ", "
					+ trackerCurrentReduceCapacity + ","
					+ trackerRunningReduces + "] -> ["
					+ (trackerCurrentReduceCapacity - trackerRunningReduces)
					+ ", " + (assignedTasks.size() - assignedMaps) + "]");*/
		}

		return assignedTasks;
	}

	private boolean exceededPadding(boolean isMapTask,
			ClusterStatus clusterStatus, int maxTaskTrackerSlots) {
		int numTaskTrackers = clusterStatus.getTaskTrackers();
		int totalTasks = (isMapTask) ? clusterStatus.getMapTasks()
				: clusterStatus.getReduceTasks();
		int totalTaskCapacity = isMapTask ? clusterStatus.getMaxMapTasks()
				: clusterStatus.getMaxReduceTasks();

		Collection<JobInProgress> jobQueue = jobQueueJobInProgressListener
				.getJobQueue();

		boolean exceededPadding = false;
		synchronized (jobQueue) {
			int totalNeededTasks = 0;
			for (JobInProgress job : jobQueue) {
				if (job.getStatus().getRunState() != JobStatus.RUNNING
						|| job.numReduceTasks == 0) {
					continue;
				}

				//
				// Beyond the highest-priority task, reserve a little
				// room for failures and speculative executions; don't
				// schedule tasks to the hilt.
				//
				totalNeededTasks += isMapTask ? job.desiredMaps() : job
						.desiredReduces();
				int padding = 0;
				if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
					padding = Math.min(maxTaskTrackerSlots,
							(int) (totalNeededTasks * padFraction));
				}
				if (totalTasks + padding >= totalTaskCapacity) {
					exceededPadding = true;
					break;
				}
			}
		}

		return exceededPadding;
	}

	@Override
	public synchronized Collection<JobInProgress> getJobs(String queueName) {
		return jobQueueJobInProgressListener.getJobQueue();
	}

	@Override
	public void addJob(JobID jobId, JobInProgress job) {
		for (JobInProgressListener listener : jobInProgressListeners) {
			try {
				listener.jobAdded(job);
			} catch (IOException ioe) {
//				LOG.warn("Failed to add and so skipping the job : "
//						+ job.getJobID() + ". Exception : " + ioe);
			}
		}
	}

	@Override
	public void addJobInProgressListener(JobInProgressListener listener) {
		jobInProgressListeners.add(listener);
	}

	@Override
	public void removeJobInProgressListener(JobInProgressListener listener) {
		jobInProgressListeners.remove(listener);
	}
}