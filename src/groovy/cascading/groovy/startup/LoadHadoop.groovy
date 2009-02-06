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

package cascading.groovy.startup

class LoadHadoop
{

  void register()
  {
    if( System.getenv('HADOOP_HOME') == null )
      return;

    def loader = this.class.classLoader.rootLoader

    def hadoopHome = new File(System.getenv('HADOOP_HOME'))

    loadJarsInDir(new File(hadoopHome, 'lib'), loader, '.*\\.jar$')

    File hadoopBuild = new File(hadoopHome, 'build')

    if( hadoopBuild.exists() )
    {
      loadJarsInDir(hadoopBuild, loader, '.*-core\\.jar$')
    } else
    {
      loadJarsInDir(hadoopHome, loader, '.*-core\\.jar$')
    }

    def confdir

    if( System.getenv('HADOOP_CONF_DIR') )
    {
      confdir = new File(System.getenv('HADOOP_CONF_DIR'))
    } else
    {
      confdir = new File(System.getenv('HADOOP_HOME'), 'conf')
    }

    loader.addURL(confdir.toURI().toURL())
  }

  private def loadJarsInDir(File jardir, loader, pattern)
  {
    def jars = jardir.listFiles().findAll { it.name.matches(pattern) }
    jars.each {
      loader.addURL(it.toURI().toURL())
    }
  }

}