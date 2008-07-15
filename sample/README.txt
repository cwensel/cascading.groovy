To run these samples, you first must install Cascading.groovy and Groovy, see the README.txt
in the project root directory.

To run a sample, you may first need to increase jvm memory options (this is especially true with Hadoop >= 0.17)

On unix (bash)
 $ export JAVA_OPTS=-Xmx512m

See the Groovy docs for other methods.

To run:

 $ groovy some-sample.groovy

where some-sample.groovy is one of the .groovy files in this directory.