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
import java.util.List;


/**
 * Abstracts the resource loading mechanism by DynamicClassLoader
 */
public interface ResourceLoader {

    /**
     * The name is the fully qualified class resource name. Qualifier separator is '/'. It is called by DynamicClassLoader when a class
     * needs to be loaded. '.class' suffix if appended by DynamicClassLoader.
     * 
     * @param name
     * @return ItemMeta
     */
    Resource loadResource(String name);

    /**
     * Called by DynamicClassLoader when findResource is invoked.
     * 
     * @param name
     * @return URL
     */
    URL getResource(String name);

    /**
     * Called by DynamicClassLoader when findResources is invoked.
     * 
     * @param name
     * @return List<URL>
     */
    List<URL> getResources(String name);

    /**
     * Releases retained resources
     */
    void release();

}
