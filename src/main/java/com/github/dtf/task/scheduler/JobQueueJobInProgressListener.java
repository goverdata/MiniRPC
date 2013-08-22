package com.github.dtf.task.scheduler;

import java.io.IOException;
import java.util.Collection;

import com.github.dtf.JobInProgress;
import com.github.dtf.JobInProgressListener;

public class JobQueueJobInProgressListener extends JobInProgressListener {

	@Override
	public void jobAdded(JobInProgress job) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void jobRemoved(JobInProgress job) {
		// TODO Auto-generated method stub

	}

	public Collection<JobInProgress> getJobQueue() {
		// TODO Auto-generated method stub
		return null;
	}

}
