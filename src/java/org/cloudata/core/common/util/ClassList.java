/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudata.core.common.util;

import java.io.*;
import java.util.*;
import java.util.jar.*;
import java.net.*;

/**
 * This abstract class can be used to obtain a list of all classes in a classpath.
 *
 * <em>Caveat:</em> When used in environments which utilize multiple class loaders--such as
 * a J2EE Container like Tomcat--it is important to select the correct classloader
 * otherwise the classes returned, if any, will be incompatible with those declared
 * in the code employing this class lister.
 * to get a reference to your classloader within an instance method use:
 *  <code>this.getClass().getClassLoader()</code> or
 *  <code>Thread.currentThread().getContextClassLoader()</code> anywhere else
 * <p>
 * @author Kris Dover <krisdover@hotmail.com>
 * @version 0.2.0
 * @since   0.1.0
 */
public abstract class ClassList{ 
  /**
   * Searches the classpath for all classes matching a specified search criteria, 
   * returning them in a map keyed with the interfaces they implement or null if they
   * have no interfaces. The search criteria can be specified via interface, package
   * and jar name filter arguments
   * <p>
   * @param classLoader       The classloader whose classpath will be traversed
   * @param interfaceFilter   A Set of fully qualified interface names to search for
   *                          or null to return classes implementing all interfaces
   * @param packageFilter     A Set of fully qualified package names to search for or
   *                          or null to return classes in all packages
   * @param jarFilter         A Set of jar file names to search for or null to return
   *                          classes from all jars
   * @return A Map of a Set of Classes keyed to their interface names
   *
   * @throws ClassNotFoundException if the current thread's classloader cannot load
   *                                a requested class for any reason
   */
  public static List<Class> findClasses(ClassLoader classLoader,
                                                    Class classType,
                                                    String packageFilter,
                                                    Set<String> jarFilter) throws ClassNotFoundException {
    List<Class> result = new ArrayList<Class>();
    Object[] classPaths;
    try{
      // get a list of all classpaths
      classPaths = ((java.net.URLClassLoader) classLoader).getURLs();
    }catch(ClassCastException cce){
      // or cast failed; tokenize the system classpath
      classPaths = System.getProperty("java.class.path", "").split(File.pathSeparator);      
    }
    
    for(int h = 0; h < classPaths.length; h++){
      Enumeration files = null;
      JarFile module = null;
      // for each classpath ...
      File classPath = new File( (URL.class).isInstance(classPaths[h]) ?
                                  ((URL)classPaths[h]).getFile() : classPaths[h].toString() );
      if( classPath.isDirectory() && jarFilter == null){   // is our classpath a directory and jar filters are not active?
        List<String> dirListing = new ArrayList();
        // get a recursive listing of this classpath
        recursivelyListDir(dirListing, classPath, new StringBuffer() );
        // an enumeration wrapping our list of files
        files = Collections.enumeration( dirListing );
      }else if( classPath.getName().endsWith(".jar") ){    // is our classpath a jar?
        // skip any jars not list in the filter
        if( jarFilter != null && !jarFilter.contains( classPath.getName() ) ){
          continue;
        }
        try{
          // if our resource is a jar, instantiate a jarfile using the full path to resource
          module = new JarFile( classPath );
        }catch (MalformedURLException mue){
          throw new ClassNotFoundException("Bad classpath. Error: " + mue.getMessage());
        }catch (IOException io){
          /*
          throw new ClassNotFoundException("jar file '" + classPath.getName() + 
            "' could not be instantiate from file path. Error: " + io.getMessage());
            */
          continue;
        }
        // get an enumeration of the files in this jar
        files = module.entries();
      }
      
      // for each file path in our directory or jar
      while( files != null && files.hasMoreElements() ){
        // get each fileName
        String fileName = files.nextElement().toString();
        // we only want the class files
        if( fileName.endsWith(".class") ){
          // convert our full filename to a fully qualified class name
          String className = fileName.replaceAll("/", ".").substring(0, fileName.length() - 6);
          // debug class list
          // skip any classes in packages not explicitly requested in our package filter          
          if( packageFilter != null) {
            if(className.indexOf(".") < 0) {
              continue;
            }
            if(className.indexOf(packageFilter) < 0){
              continue;
            }
          }      
          
          // get the class for our class name
          Class theClass = null;
          try{
            theClass = Class.forName(className, false, classLoader);
          }catch(NoClassDefFoundError e){
            //System.out.println("Skipping class '" + className + "' for reason " + e.getMessage());
            continue;
          }
          // skip interfaces
          if( theClass.isInterface() ){
            continue;
          }
          
          if(isMatch(classType, theClass)) {
            result.add(theClass);
          }
        }
       }
      
      // close the jar if it was used
      if(module != null){
        try{
          module.close();
        }catch(IOException ioe){
          throw new ClassNotFoundException("The module jar file '" + classPath.getName() +
            "' could not be closed. Error: " + ioe.getMessage());
        }
      }
      
    } // end for loop
    
    return result;
  } // end method
  
  private static boolean isMatch(Class targetClass, Class loadedClass) {
    if(targetClass.equals(loadedClass)) {
      return true;
    }
    
    //then get an array of all the interfaces in our class
    Class [] classInterfaces = loadedClass.getInterfaces();
    if(classInterfaces != null) {
      for (Class eachInterfaceClass: classInterfaces) {
        if(eachInterfaceClass.equals(targetClass)) {
          return true;
        } 
        
        if(isMatch(targetClass, eachInterfaceClass)) {
          return true;
        }
      }
    }
    
    Class superClass = loadedClass.getSuperclass();
    if(superClass != null) {
      if(isMatch(targetClass, superClass)) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Recursively lists a directory while generating relative paths. This is a helper function for findClasses.
   * Note: Uses a StringBuffer to avoid the excessive overhead of multiple String concatentation
   *
   * @param dirListing     A list variable for storing the directory listing as a list of Strings
   * @param dir                 A File for the directory to be listed
   * @param relativePath A StringBuffer used for building the relative paths
   */
  private static void recursivelyListDir(List<String> dirListing, File dir, StringBuffer relativePath){
    int prevLen; // used to undo append operations to the StringBuffer
    
    // if the dir is really a directory 
    if( dir.isDirectory() ){
      // get a list of the files in this directory
      File[] files = dir.listFiles();
      // for each file in the present dir
      for(int i = 0; i < files.length; i++){
        // store our original relative path string length
        prevLen = relativePath.length();
        // call this function recursively with file list from present
        // dir and relateveto appended with present dir
        recursivelyListDir(dirListing, files[i], relativePath.append( prevLen == 0 ? "" : "/" ).append( files[i].getName() ) );
        //  delete subdirectory previously appended to our relative path
        relativePath.delete(prevLen, relativePath.length());
      }
    }else{
      // this dir is a file; append it to the relativeto path and add it to the directory listing
      dirListing.add( relativePath.toString() );
    }
  }
}

