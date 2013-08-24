package com.github.dtf.task.scheduler;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.github.dtf.JobID;
import com.github.dtf.JobInProgress;
import com.github.dtf.JobInProgressListener;
import com.github.dtf.TaskTrackerManager;
import com.github.dtf.task.Task;
import com.github.dtf.task.TaskTrackerStatus;

public abstract class TaskScheduler {

	protected TaskTrackerManager taskTrackerManager;

	/*
	 * public Configuration getConf() { return conf; }
	 * 
	 * public void setConf(Configuration conf) { this.conf = conf; }
	 */

	public synchronized void setTaskTrackerManager(
			TaskTrackerManager taskTrackerManager) {
		this.taskTrackerManager = taskTrackerManager;
	}

	/**
	 * Lifecycle method to allow the scheduler to start any work in separate
	 * threads.
	 * 
	 * @throws IOException
	 */
	public void start() throws IOException {
		// do nothing
	}

	/**
	 * Lifecycle method to allow the scheduler to stop any work it is doing.
	 * 
	 * @throws IOException
	 */
	public void terminate() throws IOException {
		// do nothing
	}

	/**
	 * Returns the tasks we'd like the TaskTracker to execute right now.
	 * 
	 * @param taskTracker
	 *            The TaskTracker for which we're looking for tasks.
	 * @return A list of tasks to run on that TaskTracker, possibly empty.
	 */
	public abstract List<Task> assignTasks(TaskTrackerStatus taskTracker)
			throws IOException;

	/**
	 * Returns a collection of jobs in an order which is specific to the
	 * particular scheduler.
	 * 
	 * @param queueName
	 * @return
	 */
	public abstract Collection<JobInProgress> getJobs(String queueName);

	public abstract void addJob(JobID jobId, JobInProgress job);

	/**
	 * Registers a {@link JobInProgressListener} for updates from this
	 * {@link TaskTrackerManager}.
	 * 
	 * @param jobInProgressListener
	 *            the {@link JobInProgressListener} to add
	 */
	public abstract void addJobInProgressListener(JobInProgressListener listener);

	/**
	 * Unregisters a {@link JobInProgressListener} from this
	 * {@link TaskTrackerManager}.
	 * 
	 * @param jobInProgressListener
	 *            the {@link JobInProgressListener} to remove
	 */
	public abstract void removeJobInProgressListener(
			JobInProgressListener listener);
}
