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
 * (c) Copyright IBM Corp. 2012
 * 
 * The source code for this program is not published or otherwise divested of
 * its trade secrets, irrespective of what has been deposited with the U.S.
 * Copyright Office.
 */

package org.apache.cassandra.classloader;

import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureClassLoader;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.Manifest;



/**
 * Flexible class-loader that allows class loading from various locations abstracted by ResourceLoader. It also allows explicit resource
 * release.
 */
public class DynamicClassLoader extends SecureClassLoader {

    protected ResourceLoader loader;

    /**
     * @param parent
     * @param loader
     * @throws IllegalStateException
     */
    public DynamicClassLoader(ClassLoader parent, ResourceLoader loader) throws IllegalStateException {
        super(parent);
        if (loader == null)
            throw new IllegalArgumentException("loader can not be null");
        this.loader = loader;
    }

    /**
     * @param loader
     * @throws IllegalStateException
     */
    public DynamicClassLoader(ResourceLoader loader) throws IllegalStateException {
        super(null);
        if (loader == null)
            throw new IllegalArgumentException("loader can not be null");
        this.loader = loader;
    }

    /**
     * @return Returns the loader
     */
    public ResourceLoader getLoader() {
        return this.loader;
    }

    /**
     * @throws ReleaseException
     */
    public void releaseResources() {
        loader.release();
    }

    /**
     * @param name
     * @return Class
     * @throws ClassNotFoundException
     * @see java.lang.ClassLoader#findClass(java.lang.String)
     */
    protected Class<?> findClass(final String name) throws ClassNotFoundException {
        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<Class<?>>() {

                public Class<?> run() throws Exception {
                    Resource resource = loader.loadResource(name.replace('.', '/').concat(".class"));
                    return define(name, resource);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new ClassNotFoundException(e.getMessage(), e.getException());
        } catch (RuntimeException e) {
            throw new ClassNotFoundException(e.getMessage(), e);
        }
    }

    /**
     * @param name
     * @param resource
     * @param metaInf
     * @return Class
     * @throws ClassNotFoundException
     */
    private Class<?> define(String name, Resource resource) throws ClassNotFoundException {
        int lastDotPos = name.lastIndexOf('.');
        if (resource == null || resource.getRawData() == null) {
            throw new ClassNotFoundException(name);
        }

        if (lastDotPos > -1) {
            String pkg = name.substring(0, name.lastIndexOf('.'));
            definePackage(pkg, resource);
        }
        Class<?> clazz = super.defineClass(name, resource.getRawData(), 0, resource.getRawData().length, resource.getCodeSource());
        return clazz;
    }

    /**
     * @param pkg
     * @param entity
     * @param metaInf
     * @return Package
     */
    private Package definePackage(String pkg, Resource entity) {

        String specTitle = null;
        String specVersion = null;
        String specVendor = null;
        String implTitle = null;
        String implVersion = null;
        String implVendor = null;
        String sealed = null;
        URL sealBase = null;

        Manifest manifest = entity.getManifest();

        if (manifest != null) {
            Attributes attr = manifest.getAttributes(pkg + '/');
            Attributes main = manifest.getMainAttributes();

            specTitle = getPackageEntry(specTitle, Name.SPECIFICATION_TITLE, attr, main);
            specVersion = getPackageEntry(specVersion, Name.SPECIFICATION_VERSION, attr, main);
            specVendor = getPackageEntry(specVendor, Name.SPECIFICATION_VENDOR, attr, main);
            implTitle = getPackageEntry(implTitle, Name.IMPLEMENTATION_TITLE, attr, main);
            implVersion = getPackageEntry(implVersion, Name.IMPLEMENTATION_VERSION, attr, main);
            implVendor = getPackageEntry(implVendor, Name.IMPLEMENTATION_VENDOR, attr, main);
            sealed = getPackageEntry(sealed, Name.SEALED, attr, main);
            if ("true".equalsIgnoreCase(sealed)) {
                sealBase = entity.getCodeSource().getLocation();
            }
        }

        Package definedPkg = getPackage(pkg);
        if (definedPkg == null) {
            definedPkg = definePackage(pkg, specTitle, specVersion, specVendor, implTitle, implVersion, implVendor, sealBase);
        }
        return definedPkg;
    }

    /**
     * @param item
     * @param name
     * @param attr
     * @param main
     * @return String
     */
    private String getPackageEntry(String item, Name name, Attributes attr, Attributes main) {
        if (attr != null) {
            item = attr.getValue(name);
            if (main != null && item == null) {
                item = main.getValue(name);
            }
        }
        return item;
    }

    /**
     * @param codesource
     * @return PermissionCollection
     * @see java.security.SecureClassLoader#getPermissions(java.security.CodeSource)
     */
    protected PermissionCollection getPermissions(final CodeSource codesource) {
        PermissionCollection permissions = AccessController.doPrivileged(new PrivilegedAction<PermissionCollection>() {

            public PermissionCollection run() {
                PermissionCollection permissions = Policy.getPolicy().getPermissions(codesource);
                return permissions;
            }
        });
        return permissions;
    }

    /**
     * @param name
     * @return URL
     * @see java.lang.ClassLoader#findResource(java.lang.String)
     */
    protected URL findResource(final String name) {
        URL url = loader.getResource(name);
        return url;
    }

    /**
     * @see java.lang.ClassLoader#findResources(java.lang.String)
     */
    protected Enumeration<URL> findResources(final String pattern) throws IOException {
        final List<URL> urls = loader.getResources(pattern);
        return (urls == null) ? null : new Enumeration<URL>() {
            Iterator<URL> iter = urls.iterator();

            public boolean hasMoreElements() {
                return iter.hasNext();
            }

            public URL nextElement() {
                return iter.next();
            }
        };
    }

    /**
     * @see java.lang.Object#finalize()
     */
    protected void finalize() {
        releaseResources();
    }

    /**
     * @return int
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return loader.hashCode();
    }

    /**
     * @param obj
     * @return boolean
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        return (obj != null && obj instanceof DynamicClassLoader) ? loader.equals(((DynamicClassLoader) obj).loader) : false;
    }

    /**
     * @return String
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(DynamicClassLoader.class).append(": ResourceLoader = ").append((loader == null) ? null : loader.toString());
        return sb.toString();
    }
}
