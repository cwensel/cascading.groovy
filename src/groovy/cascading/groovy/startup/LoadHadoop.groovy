/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
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

    if( System.getenv('HADOOP_CONF') )
    {
      confdir = new File(System.getenv('HADOOP_CONF'))
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