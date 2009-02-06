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

def cascading = new Cascading()
def builder = cascading.builder();

Cascade fetchSort = builder.cascade("fetchSort")
  {
    flow("fetch", skipIfSinkExists: true)
      {
        source('http://www.census.gov/genealogy/names/dist.all.last', scheme: text())

        copy()

        sink('output/imported', scheme: text())
      }

    flow("filter")
      {
        source('output/imported', scheme: text())

        cut(/\s+/, results: [1])
        group()

        sink('output/sorted', scheme: text(), delete: true)
      }
  }

cascading.enableInfoLogging()

try
{
  fetchSort.complete()
}
catch (Exception exception)
{
  exception.printStackTrace()
}
