#!/usr/bin/env groovy

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

println("Installing Cascading Groovy Shell Extensions")

def installRoot = "${System.properties[ "user.home" ]}/.groovy"

File groovyHome = new File(installRoot);
File groovyLib = new File(groovyHome, "lib");

println "  installing to: ${groovyHome}"

def ant = new AntBuilder()

groovyLib.mkdirs();

println "  copying files to: ${groovyLib}"

// remove older jars, catch all cascading.jars
// need to track a manifest instead
ant.delete()
  {
    fileset(dir: groovyLib, includes: "cascading*.jar")
  }

ant.copy(todir: groovyLib)
  {
    fileset(dir: "./lib")
      {
        include(name: "*.jar")
      }
  }

println "Done"