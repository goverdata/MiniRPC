package com.github.dtf.task;

import java.util.Set;

import com.github.dtf.job.Job;

public interface Task {
	enum TaskType {
		JUNIT3, JUNIT4, PYTHON
	}

	public String getName();

	public void setName(String name);

	public String getPath();

	public void setPath(String casePath);

	public void setPath(Set<String> caseFlags);

	public Set<String> getFlag();

	public TaskType getType();

	public Job getJob();

	public void setJob(Job caseJob);
}
