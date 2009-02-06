
Thanks for using Cascading.

General Information:

  Project and contact information: http://www.cascading.org/

  This distribution includes all necessary Cascading libraries, and a simple installer.

  To install, first make sure Groovy is installed, then execute

   $ groovy setup.groovy

  This will copy all libraries to ~/.groovy/lib.

  During runtime, when instantiating cascading.groovy.Cascading(), Hadoop libraries will be registered
  with the Groovy Classloader based on your HADOOP_HOME and HADOOP_CONF.

  See the included samples to get started.

  To run, you may first need to increase jvm memory options (this is especially true with Hadoop >= 0.17)

   $ export JAVA_OPTS=-Xmx512m


License

  Copyright (c) 2009 Concurrent, Inc.

  This work has been released into the public domain
  by the copyright holder. This applies worldwide.

  In case this is not legally possible:
  The copyright holder grants any entity the right
  to use this work for any purpose, without any
  conditions, unless such conditions are required by law.