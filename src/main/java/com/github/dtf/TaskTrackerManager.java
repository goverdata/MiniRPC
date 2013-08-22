package com.github.dtf;


import java.io.IOException;

import com.github.dtf.task.scheduler.ClusterStatus;

/**
 * Manages information about the {@link TaskTracker}s running on a cluster.
 * This interface exits primarily to test the {@link JobTracker}, and is not
 * intended to be implemented by users.
 */
public interface TaskTrackerManager {

  /**
   * @return A collection of the {@link TaskTrackerStatus} for the tasktrackers
   * being managed.
   */
//  public Collection<TaskTrackerStatus> taskTrackers();
  
  /**
   * @return The number of unique hosts running tasktrackers.
   */
  public int getNumberOfUniqueHosts();
  
  /**
   * @return a summary of the cluster's status.
   */
//  public ClusterStatus getClusterStatus();


  /**
   * Return the {@link QueueManager} which manages the queues in this
   * {@link TaskTrackerManager}.
   *
   * @return the {@link QueueManager}
   */
//  public QueueManager getQueueManager();
  
  /**
   * Return the current heartbeat interval that's used by {@link TaskTracker}s.
   *
   * @return the heartbeat interval used by {@link TaskTracker}s
   */
  public int getNextHeartbeatInterval();

  /**
   * Kill the job identified by jobid
   * 
   * @param jobid
   * @throws IOException
   */
  public void killJob(JobID jobid)
      throws IOException;

  /**
   * Obtain the job object identified by jobid
   * 
   * @param jobid
   * @return jobInProgress object
   */
  public JobInProgress getJob(JobID jobid);
  
  /**
   * Initialize the Job
   * 
   * @param job JobInProgress object
   */
  public void initJob(JobInProgress job);
  
  /**
   * Fail a job.
   * 
   * @param job JobInProgress object
   */
  public void failJob(JobInProgress job);

public ClusterStatus getClusterStatus();
}
