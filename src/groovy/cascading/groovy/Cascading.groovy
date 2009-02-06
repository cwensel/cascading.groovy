/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.groovy

import cascading.groovy.startup.LoadHadoop

class Cascading
{
  // JobConf config data
  static Map defaultConfiguration;

  static {
    new LoadHadoop().register()
  }

  private Properties defaultProperties = new Properties();

  Cascading()
  {
  }

  Cascading(Map props)
  {
    props.each {key, value ->
      defaultProperties.setProperty(key, value.toString());
    }
  }

  CascadingBuilder builder()
  {
    return new CascadingBuilder(defaultProperties);
  }

  static Map getDefaultConfiguration()
  {
    if( defaultConfiguration != null )
      return defaultConfiguration;

    def JobConfClass = Class.forName("org.apache.hadoop.mapred.JobConf");
    def jobConf = JobConfClass.newInstance();

    defaultConfiguration = [:];

    jobConf.iterator().each {entry ->
      defaultConfiguration[ entry.key ] = jobConf.get(entry.key);
    }

    return defaultConfiguration;
  }

  void disableLogging()
  {
    setLog4JLevel("cascading", null)
  }

  void enableInfoLogging()
  {
    setLog4JLevel("cascading", "INFO")
  }

  void enableDebugLogging()
  {
    setLog4JLevel("cascading", "DEBUG")
  }

  private def setLog4JLevel(def name, def level)
  {
    def LoggerClass = this.class.classLoader.loadClass("org.apache.log4j.Logger")
    def LevelClass = this.class.classLoader.loadClass("org.apache.log4j.Level")
    def logger = LoggerClass.invokeMethod("getLogger", name)
    def infoLevel = level != null ? LevelClass.getField(level).get(null) : null

    logger.setLevel(infoLevel)
  }

}