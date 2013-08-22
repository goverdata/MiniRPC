package com.github.dtf.rpc.server;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


// Sends responses of RPC back to clients.
public class Responder extends Thread {
	
  public static final Log LOG = LogFactory.getLog(Responder.class);
  
  private final Selector writeSelector;
  private int pending;         // connections waiting to register
  
  final static int PURGE_INTERVAL = 900000; // 15mins
  AbstractServer server;
  public Responder(AbstractServer server) throws IOException {
	  this.server = server;
    this.setName("IPC Server Responder");
    this.setDaemon(true);
    writeSelector = Selector.open(); // create a selector
    pending = 0;
  }

  @Override
  public void run() {
    LOG.info(getName() + ": starting");
    //SERVER.set(AbstractServer.this);
    try {
      doRunLoop();
    } finally {
      LOG.info("Stopping " + this.getName());
      try {
        writeSelector.close();
      } catch (IOException ioe) {
        LOG.error("Couldn't close write selector in " + this.getName(), ioe);
      }
    }
  }
  
  private void doRunLoop() {
    long lastPurgeTime = 0;   // last check for old calls.

    while (server.isRunning()) {
      try {
        waitPending();     // If a channel is being registered, wait.
        writeSelector.select(PURGE_INTERVAL);
        Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
        while (iter.hasNext()) {
          SelectionKey key = iter.next();
          iter.remove();
          try {
            if (key.isValid() && key.isWritable()) {
                doAsyncWrite(key);
            }
          } catch (IOException e) {
            LOG.info(getName() + ": doAsyncWrite threw exception " + e);
          }
        }
        long now = System.currentTimeMillis();
        if (now < lastPurgeTime + PURGE_INTERVAL) {
          continue;
        }
        lastPurgeTime = now;
        //
        // If there were some calls that have not been sent out for a
        // long time, discard them.
        //
        if(LOG.isDebugEnabled()) {
          LOG.debug("Checking for old call responses.");
        }
        ArrayList<Call> calls;
        
        // get the list of channels from list of keys.
        synchronized (writeSelector.keys()) {
          calls = new ArrayList<Call>(writeSelector.keys().size());
          iter = writeSelector.keys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            Call call = (Call)key.attachment();
            if (call != null && key.channel() == call.getConnection().getChannel()) { 
              calls.add(call);
            }
          }
        }
        
        for(Call call : calls) {
          try {
            doPurge(call, now);
          } catch (IOException e) {
            LOG.warn("Error in purging old calls " + e);
          }
        }
      } catch (OutOfMemoryError e) {
        //
        // we can run out of memory if we have too many threads
        // log the event and sleep for a minute and give
        // some thread(s) a chance to finish
        //
        LOG.warn("Out of Memory in server select", e);
        try { Thread.sleep(60000); } catch (Exception ie) {}
      } catch (Exception e) {
        LOG.warn("Exception in Responder", e);
      }
    }
  }

  private void doAsyncWrite(SelectionKey key) throws IOException {
    Call call = (Call)key.attachment();
    if (call == null) {
      return;
    }
    if (key.channel() != call.getConnection().getChannel()) {
      throw new IOException("doAsyncWrite: bad channel");
    }

    synchronized(call.getConnection().responseQueue) {
      if (processResponse(call.getConnection().responseQueue, false)) {
        try {
          key.interestOps(0);
        } catch (CancelledKeyException e) {
          /* The Listener/reader might have closed the socket.
           * We don't explicitly cancel the key, so not sure if this will
           * ever fire.
           * This warning could be removed.
           */
          LOG.warn("Exception while changing ops : " + e);
        }
      }
    }
  }

  //
  // Remove calls that have been pending in the responseQueue 
  // for a long time.
  //
  private void doPurge(Call call, long now) throws IOException {
    LinkedList<Call> responseQueue = call.getConnection().responseQueue;
    synchronized (responseQueue) {
      Iterator<Call> iter = responseQueue.listIterator(0);
      while (iter.hasNext()) {
        call = iter.next();
        if (now > call.getTimestamp() + PURGE_INTERVAL) {
          server.closeConnection(call.getConnection());
          break;
        }
      }
    }
  }

  // Processes one response. Returns true if there are no more pending
  // data for this channel.
  //
  private boolean processResponse(LinkedList<Call> responseQueue,
                                  boolean inHandler) throws IOException {
    boolean error = true;
    boolean done = false;       // there is more data for this channel.
    int numElements = 0;
    Call call = null;
    try {
      synchronized (responseQueue) {
        //
        // If there are no items for this channel, then we are done
        //
        numElements = responseQueue.size();
        if (numElements == 0) {
          error = false;
          return true;              // no more data for this channel.
        }
        //
        // Extract the first call
        //
        call = responseQueue.removeFirst();
        SocketChannel channel = call.getConnection().getChannel();
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": responding to #" + call.getCallId() + " from " +
                    call.getConnection());
        }
        //
        // Send as much data as we can in the non-blocking fashion
        //
        int numBytes = server.channelWrite(channel, call.getRpcResponse());
        if (numBytes < 0) {
          return true;
        }
        if (!call.getRpcResponse().hasRemaining()) {
          call.getConnection().decRpcCount();
          if (numElements == 1) {    // last call fully processes.
            done = true;             // no more data for this channel.
          } else {
            done = false;            // more calls pending to be sent.
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to #" + call.getCallId() + " from " +
                      call.getConnection() + " Wrote " + numBytes + " bytes.");
          }
        } else {
          //
          // If we were unable to write the entire response out, then 
          // insert in Selector queue. 
          //
          call.getConnection().responseQueue.addFirst(call);
          
          if (inHandler) {
            // set the serve time when the response has to be sent later
            call.setTimestamp(System.currentTimeMillis());
            
            incPending();
            try {
              // Wakeup the thread blocked on select, only then can the call 
              // to channel.register() complete.
              writeSelector.wakeup();
              channel.register(writeSelector, SelectionKey.OP_WRITE, call);
            } catch (ClosedChannelException e) {
              //Its ok. channel might be closed else where.
              done = true;
            } finally {
              decPending();
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to #" + call.getCallId() + " from " +
                      call.getConnection() + " Wrote partial " + numBytes + 
                      " bytes.");
          }
        }
        error = false;              // everything went off well
      }
    } finally {
      if (error && call != null) {
        LOG.warn(getName()+", call " + call + ": output error");
        done = true;               // error. no more data for this channel.
        server.closeConnection(call.getConnection());
      }
    }
    return done;
  }

  //
  // Enqueue a response from the application.
  //
  void doRespond(Call call) throws IOException {
    synchronized (call.getConnection().responseQueue) {
      call.getConnection().responseQueue.addLast(call);
      if (call.getConnection().responseQueue.size() == 1) {
        processResponse(call.getConnection().responseQueue, true);
      }
    }
  }

  private synchronized void incPending() {   // call waiting to be enqueued.
    pending++;
  }

  private synchronized void decPending() { // call done enqueueing.
    pending--;
    notify();
  }

  private synchronized void waitPending() throws InterruptedException {
    while (pending > 0) {
      wait();
    }
  }
}