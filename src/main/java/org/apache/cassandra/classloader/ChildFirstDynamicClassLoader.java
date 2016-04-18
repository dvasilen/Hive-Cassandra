/**
 * Title: 
 * 
 * Description: 
 * 
 * IBM Confidential
 * 
 * OCO Source Materials
 * 
 * IBM SPSS Products: Analytic Server
 * (c) Copyright IBM Corp. 2015
 * 
 * The source code for this program is not published or otherwise divested of
 * its trade secrets, irrespective of what has been deposited with the U.S.
 * Copyright Office.
 */

package org.apache.cassandra.classloader;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

 

/**
 * Flexible child-first class-loader that allows class loading from various locations abstracted by ResourceLoader. It also allows explicit resource
 * release.
 */
public class ChildFirstDynamicClassLoader extends DynamicClassLoader {
    private static final Logger log = Logger.getLogger(ChildFirstDynamicClassLoader.class);
    private Map<String, Class<?>> classes = new HashMap<String, Class<?>>();

    /**
     * @param parent
     * @param loader
     * @throws IllegalStateException
     */
    public ChildFirstDynamicClassLoader(ClassLoader parent, ResourceLoader loader) throws IllegalStateException {
        super(parent, loader);
    }

    /**
     * @param loader
     * @throws IllegalStateException
     */
    public ChildFirstDynamicClassLoader(ResourceLoader loader) throws IllegalStateException {
        super(null, loader);
    }

    /**
     * @param obj
     * @return boolean
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        return (obj != null && obj instanceof ChildFirstDynamicClassLoader) ? loader.equals(((ChildFirstDynamicClassLoader) obj).loader)
                : false;
    }

    /**
     * @return String
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(ChildFirstDynamicClassLoader.class).append(": ResourceLoader = ").append((loader == null) ? null : loader.toString());
        return sb.toString();
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {

        synchronized (this) {
            if (classes.containsKey(name)) {
                return classes.get(name);
            }
            try {
                if (isLoadable(name)) {
                    Class<?> c = this.findClass(name);
                    if (c != null) {
                        if (resolve) {
                            resolveClass(c);
                        }
                        classes.put(name, c);
                        log.debug("name:" + name);
                        return c;
                    }
                }
            } catch (Exception e) {
            }
        }

        Class<?> c = findLoadedClass(name);
        if (c == null) {
            if (getParent() != null) {
                try {
                    c = getParent().loadClass(name);
                } catch (Exception e) {
                    //DO NOTHING
                }
            }
            if (c == null) {
                c = getSystemClassLoader().loadClass(name);
            }
        }

        if (resolve) {
            resolveClass(c);
        }

        return c;
    }

    @Override
    public URL getResource(String name) {
        URL url = findResource(name);

        if (url == null) {
            url = getParent().getResource(name);

            if (url == null) {
                url = getSystemClassLoader().getResource(name);
            }
        }

        return url;
    }

    private boolean isLoadable(String name) {
        String setting = "com.google.common";
        if (setting != null && !setting.isEmpty()) {
            String[] array = setting.split(";");
            if (array != null && array.length > 0) {
                for (String packaage : array) {
                    packaage = packaage.trim();
                    if (!packaage.isEmpty() && name.startsWith(packaage)) {
                        return true;
                    }
                }
            }
        }
        String[] loadableHeader = {"com.google.common"};
        for (String header : loadableHeader) {
            if (name.startsWith(header)) {
                return true;
            }
        }
        return false;
    }
}
