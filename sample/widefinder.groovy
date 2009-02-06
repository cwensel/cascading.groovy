import cascading.cascade.Cascade
import cascading.groovy.Cascading

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

def dataUrl = 'http://files.cascading.org/apache.200.txt.gz'

String logs = 'output/logs/'
String output = 'output/results'

def APACHE_COMMON_REGEX = /^([^ ]*) +[^ ]* +[^ ]* +\[([^]]*)\] +\"([^ ]*) ([^ ]*) [^ ]*\" ([^ ]*) ([^ ]*).*$/
def APACHE_COMMON_GROUPS = [1, 2, 3, 4, 5, 6]
def APACHE_COMMON_FIELDS = ["ip", "time", "method", "url", "status", "size"]

//def URL_PATTERN = /\/ongoing\/When\/\d\d\dx\/\d\d\d\d\/\d\d\/\d\d\/[^ .]+/
def URL_PATTERN = /^\/archives\/.*$/

def cascading = new Cascading()
def builder = cascading.builder();

Cascade cascade = builder("widefinder")
  {

    flow("fetcher", skipIfSinkExists: true) // no unnecessary polling
      {
        source(dataUrl) // gz is assumed text scheme
        copy()
        sink(logs) // inherits scheme from source
      }

    flow("counter")
      {
        source(logs, scheme: text())

        // parse apache log, given regex groups are matched with respective field names
        regexParser(pattern: APACHE_COMMON_REGEX, groups: APACHE_COMMON_GROUPS, declared: APACHE_COMMON_FIELDS)

        // throw away tuples that don't match
        filter(arguments: ["url"], pattern: URL_PATTERN)

        // throw away unused fields
        project(arguments: ["url"])

        group(groupBy: ["url"])

        // creates 'count' field, by default
        count()

        // group/sort on 'count', reverse the sort order
        group(["count"], reverse: true)

        sink(output, delete: true)
      }
  }

cascading.enableInfoLogging()

try
{
  cascade.complete() // execute the flow
}
catch (Exception exception)
{
  exception.printStackTrace()
}
