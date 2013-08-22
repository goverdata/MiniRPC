package com.github.dtf.conf;

public interface Configurable {

	  /** Set the configuration to be used by this object. */
	  void setConf(Configuration conf);

	  /** Return the configuration used by this object. */
	  Configuration getConf();
	}