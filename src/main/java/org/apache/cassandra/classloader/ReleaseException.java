/**
 * IBM Confidential
 * 
 * OCO Source Materials
 * 
 * IBM SPSS Products: Analytic Server
 * (c) Copyright IBM Corp. 2012
 * 
 * The source code for this program is not published or otherwise divested of
 * its trade secrets, irrespective of what has been deposited with the U.S.
 * Copyright Office.
 */

package org.apache.cassandra.classloader;


public class ReleaseException extends RuntimeException {
    private static final long serialVersionUID = -9171597394523745343L;

    /**
     * 
     */
    public ReleaseException() {
    }

    /**
     * @param message
     */
    public ReleaseException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public ReleaseException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public ReleaseException(String message, Throwable cause) {
        super(message, cause);
    }

}
