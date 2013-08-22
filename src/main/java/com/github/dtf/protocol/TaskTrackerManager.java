package com.github.dtf.protocol;

import java.io.IOException;
import java.util.Collection;
//
//import org.apache.hadoop.mapred.ClusterStatus;
//import org.apache.hadoop.mapred.JobID;
//import org.apache.hadoop.mapred.JobInProgress;
//import org.apache.hadoop.mapred.JobInProgressListener;
//import org.apache.hadoop.mapred.JobTracker;
//import org.apache.hadoop.mapred.QueueManager;
//import org.apache.hadoop.mapred.TaskAttemptID;
//import org.apache.hadoop.mapred.TaskTracker;
//import org.apache.hadoop.mapred.TaskTrackerManager;
//import org.apache.hadoop.mapred.TaskTrackerStatus;

/**
 * Manages information about the {@link TaskTracker}s running on a cluster.
 * This interface exits primarily to test the {@link JobTracker}, and is not
 * intended to be implemented by users.
 */
interface TaskTrackerManager {

  /**
   * @return A collection of the {@link TaskTrackerStatus} for the tasktrackers
   * being managed.
   */
  public Collection<TaskTrackerStatus> taskTrackers();
  
  /**
   * @return The number of unique hosts running tasktrackers.
   */
  public int getNumberOfUniqueHosts();
  
  /**
   * @return a summary of the cluster's status.
   */
  public ClusterStatus getClusterStatus();

  /**
   * Registers a {@link JobInProgressListener} for updates from this
   * {@link TaskTrackerManager}.
   * @param jobInProgressListener the {@link JobInProgressListener} to add
   */
  public void addJobInProgressListener(JobInProgressListener listener);

  /**
   * Unregisters a {@link JobInProgressListener} from this
   * {@link TaskTrackerManager}.
   * @param jobInProgressListener the {@link JobInProgressListener} to remove
   */
  public void removeJobInProgressListener(JobInProgressListener listener);

  /**
   * Return the {@link QueueManager} which manages the queues in this
   * {@link TaskTrackerManager}.
   *
   * @return the {@link QueueManager}
   */
  public QueueManager getQueueManager();
  
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

  /**
   * Mark the task attempt identified by taskid to be killed
   * 
   * @param taskid task to kill
   * @param shouldFail whether to count the task as failed
   * @return true if the task was found and successfully marked to kill
   */
  public boolean killTask(TaskAttemptID taskid, boolean shouldFail)
      throws IOException;
}
