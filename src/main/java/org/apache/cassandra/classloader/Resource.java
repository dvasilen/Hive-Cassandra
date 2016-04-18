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

import java.net.URL;
import java.security.CodeSource;
import java.util.jar.Manifest;


public interface Resource {
    /**
     * @return The resource name
     */
    public String getName();

    /**
     * @return The codeSource.
     */
    public CodeSource getCodeSource();

    /**
     * @return URL
     */
    public URL getUrl();

    /**
     * @return Returns the data
     */
    public byte[] getRawData();

    /**
     * @return the Manifest pertaining to this resource. For instance of multiple resource are fetched from the same jar, the same Manifest
     *         will be returned,however in case of multiple jars there are likely different Manifests depending from where the resource is
     *         loaded
     */
    public Manifest getManifest();

}
