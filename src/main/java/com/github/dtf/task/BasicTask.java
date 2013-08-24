package com.github.dtf.task;

import java.util.Set;

import com.github.dtf.job.Job;

public abstract class BasicTask implements Task {
	/**
	 * the name of the test case
	 */
	private String _name;

	/**
	 * the path of the test case
	 */
	private String _path;

	/**
	 * the job which the test case belong to
	 */
	private Job _job;

	/**
	 * the flags of the test case;
	 */
	private Set<String> _flags;

	public BasicTask() {
	}

	public BasicTask(Job caseJob) {
		_job = caseJob;
	}

	public String getName() {
		return _name;
	}

	public void setName(String name) {
		this._name = name;
	}

	public String getPath() {
		return _path;
	}

	public void setPath(String casePath) {
		_path = casePath;
	}

	public void setPath(Set<String> caseFlags) {
		_flags = caseFlags;
	}

	public Set<String> getFlag() {
		return _flags;
	}

	public Job getJob() {
		return _job;
	}

	public void setJob(Job caseJob) {
		_job = caseJob;
	}
}
