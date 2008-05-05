import cascading.flow.Flow
import cascading.groovy.Cascading

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