package com.github.dtf;

import java.io.IOException;

public abstract class JobInProgressListener {

	/**
	 * Invoked when a new job has been added to the {@link JobTracker}.
	 * 
	 * @param job
	 *            The added job.
	 * @throws IOException
	 */
	public abstract void jobAdded(JobInProgress job) throws IOException;

	/**
	 * Invoked when a job has been removed from the {@link JobTracker}.
	 * 
	 * @param job
	 *            The removed job.
	 */
	public abstract void jobRemoved(JobInProgress job);

	/**
	 * Invoked when a job has been updated in the {@link JobTracker}. This
	 * change in the job is tracker using {@link JobChangeEvent}.
	 * 
	 * @param event
	 *            the event that tracks the change
	 */
	// public abstract void jobUpdated(JobChangeEvent event);
}
