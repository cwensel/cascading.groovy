import cascading.flow.Flow
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

// This is a query and the 'content-length' is not returned, so it must be prefetched.
File input = new File('output/fetched/fetch.txt')
String output = 'output/counted'
def dataUrl = 'http://www.i-r-genius.com/cgi-bin/lipsum.cgi?qty=400&unit=k&pl=r&ps=6&pp=n&pt=1&format=t&li=1'

if( !input.exists() ) // only fetch once
  "curl --create-dirs -o ${input} ${dataUrl}".execute().waitFor()

assert input.exists()

def cascading = new Cascading()
def builder = cascading.builder();

def assembly = builder.assembly(name: "wordcount")
  {
    eachTuple(args: ["line"], results: ["word"])
      {
        regexSplitGenerator(declared: ["word"], pattern: /[.,]*\s+/)
      }

    group(["word"])

    everyGroup(args: ["word"], results: ["word", "count"])
      {
        count()
      }

    group(["count"], reverse: true)
  }

def map = builder.map()
  {
    source(name: "wordcount")
      {
        hfs(input)
          {
            text(["line"])
          }
      }

    sink(name: "wordcount")
      {
        lfs(output, delete: true)
          {
            text()
          }
      }
  }

Flow flow = builder.flow(name: "wordcount", map: map, assembly: assembly);

cascading.enableInfoLogging()

try
{
  flow.complete() // execute the flow
}
catch (Exception exception)
{
  exception.printStackTrace()
};