/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading

import cascading.cascade.Cascade
import cascading.flow.Flow
import cascading.groovy.CascadingBuilder

class CascadeTest extends GroovyTestCase
{

  String inputFileApache10 = "build/test/data/apache.10.txt";
  String inputFileApache200 = "build/test/data/apache.200.txt";
  String outputPath = "build/test/output/flow/";

  public FlowTest()
  {

  }

  void testCopy()
  {
    def builder = new CascadingBuilder();

    Cascade cascade = builder("copy cascade")
      {
        flow("copy")
          {
            source(inputFileApache10)

            copy()

            sink(outputPath + "copy", delete: true)
          }
      }

    cascade.complete()

    verifySinks(cascade.getFlows().get(0), 10)
  }

  void testCut()
  {
    def builder = new CascadingBuilder();

    Cascade cascade = builder("cut cascade")
      {
        flow("cut")
          {
            source(inputFileApache10)

            cut(/\s+/, results: [1])

            sink(outputPath + "cut", delete: true)
          }
      }

    cascade.complete()

    verifySinks(cascade.getFlows().get(0), 10, /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)
  }

  void testCutSort()
  {
    def builder = new CascadingBuilder();

    Cascade cascade = builder("cut cascade")
      {
        flow("cut")
          {
            source(inputFileApache10)

            cut(/\s+/)
            group([0])
            count()

            sink(outputPath + "cut-sort", delete: true)
          }
      }

    cascade.complete()

    verifySinks(cascade.getFlows().get(0), 8, /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\t\d$/)
  }

  void testCutSortFull()
  {
//    Logger.getLogger("cascading").setAssertionLevel(Level.DEBUG)
    def builder = new CascadingBuilder();

    Cascade cascade = builder("cut cascade")
      {
        flow("cut flow")
          {
            map
            {
              source("cut")
                {
                  lfs(inputFileApache10)
                    {
                      text(["line"])
                    }
                }

              sink("cut")
                {
                  hfs(outputPath + "cut-sort-full", delete: true)
                    {
                      text()
                    }
                }
            }

            assembly(name: "cut")
              {
                eachTuple(args: ["line"], results: [1])
                  {
                    regexSplitter(/\s+/)
                  }

                group([0])

                everyGroup(args: [0], results: ALL)
                  {
                    count()
                  }
              }
          }
      }

    cascade.complete()

    // the sink does not share the source scheme, so the offset will show up
    verifySinks(cascade.getFlows().get(0), 8, /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\t\d$/)
  }

  void testJoinFull()
  {
//    Logger.getLogger("cascading").setAssertionLevel(Level.DEBUG)
    def builder = new CascadingBuilder();

    Cascade cascade = builder("join cascade")
      {
        flow("join flow")
          {
            map
            {
              source("first")
                {
                  lfs(inputFileApache10)
                    {
                      text(["line"])
                    }
                }

              source("second")
                {
                  lfs(inputFileApache200)
                    {
                      text(["line"])
                    }
                }

              sink("joined")
                {
                  hfs(outputPath + "joined", delete: true)
                    {
                      text()
                    }
                }
            }

            assembly(name: "joined")
              {
                branch("first")
                  {
                    eachTuple(args: ["line"], results: [1])
                      {
                        regexSplitter(/\s+/)
                      }
                  }

                branch("second")
                  {
                    eachTuple(args: ["line"], results: [1])
                      {
                        regexSplitter(/\s+/)
                      }
                  }

                join(first: [0], second: [0], declared: ["ip1", "ip2"])

              }
          }
      }

//    cascade.getFlows().get(0).writeDOT( "joinflow.dot" )
    cascade.complete()

    // the sink does not share the source scheme, so the offset will show up
    verifySinks(cascade.getFlows().get(0), 14, /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\t\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)
  }

  void testFilter()
  {
    def builder = new CascadingBuilder();

    Cascade cascade = builder("grep cascade")
      {
        flow("grep")
          {
            source(inputFileApache10)

            filter(/.*88.*/)

            sink(outputPath + "filter", delete: true)
          }
      }

    cascade.complete()

    verifySinks(cascade.getFlows().get(0), 1)
  }

  void testFilterGrouped()
  {
    def builder = new CascadingBuilder();

    Cascade cascade = builder("grep cascade")
      {
        flow("grep")
          {
            source(inputFileApache10)

            filter(/.*88.*/)
            group(["line"])

            sink(outputPath + "filter-grouped", delete: true)
          }
      }

    cascade.complete()

    verifySinks(cascade.getFlows().get(0), 1)
  }

  private def verifySinks(Flow flow, int size, String match = ".*")
  {
    return flow.sinks.values().each {tap ->

      println tap

      assertTrue("does not exist", flow.tapPathExists(tap))

      def count = 0
      flow.openTapForRead(tap).each {tuple ->
        assertTrue("does not match", tuple.toString().matches(match));
        count++;
      }

      assertEquals("wrong size", size, count)
    }
  }

}