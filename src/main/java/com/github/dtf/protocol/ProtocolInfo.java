package com.github.dtf.protocol;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ProtocolInfo {
	String protocolName(); // the name of the protocol (i.e. rpc service)

	long protocolVersion() default -1; // default means not defined use old way
}
