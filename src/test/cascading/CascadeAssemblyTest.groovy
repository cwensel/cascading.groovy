/*
 * Copyright 2009 Concurrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading

import cascading.cascade.Cascade
import cascading.flow.Flow
import cascading.groovy.CascadingBuilder

class CascadeAssemblyTest extends GroovyTestCase
{

  public CascadeAssemblyTest()
  {

  }

  void testCascade()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly(name: "path")
      {
        eachTuple(args: ["f1"], res: ["f1", "g1"])
          {
            regexParser(decl: ["g1"], pattern: /.*/, groups: [0])
          }

        group(["f1"])

        everyGroup(args: ["g1"], res: ["f1", "sum"])
          {
            sum()
          }
      }

    def map = builder.map()
      {
        source(name: "path")
          {
            hfs("input/path/", delete: true)
              {
                sequence(["f1"])
              }
          }

        sink(name: "path")
          {
            lfs("output/path", delete: true)
              {
                text()
              }
          }
      }

    Flow flow = builder.flow(name: "flow", map: map, assembly: assembly);

    assertEquals("sources", 1, flow.sources.size());
    assertEquals("sinks", 1, flow.sinks.size());

    Cascade cascade = builder.cascade(name: "cascade", flows: [flow])

    assertEquals("names", "cascade", cascade.getName())
    assertEquals("flows", 1, cascade.getFlows().size())
  }

  void testCascadeNested()
  {
    def builder = new CascadingBuilder();

    def cascade = builder.cascade("cascade")
      {
        flow("flow")
          {
            assembly(name: "path")
              {
                eachTuple(args: ["f1"], res: ["f1", "g1"])
                  {
                    regexParser(decl: ["g1"], pattern: /.*/, groups: [0])
                  }

                group(["f1"])

                everyGroup(args: ["g1"], res: ["f1", "sum"])
                  {
                    sum()
                  }
              }

            map()
              {
                source(name: "path")
                  {
                    hfs("input/path/", delete: true)
                      {
                        sequence(["f1"])
                      }
                  }

                sink(name: "path")
                  {
                    lfs("output/path", delete: true)
                      {
                        text()
                      }
                  }
              }
          }
      }

    assertEquals("names", "cascade", cascade.getName())
    assertEquals("flows", 1, cascade.getFlows().size())
  }

  void testCascadeAbbreviated()
  {
    def builder = new CascadingBuilder();

    def cascade = builder.cascade("cascade")
      {
        flow("flow")
          {
            map()
              {
                source(path: "input/path", delete: true, fields: ["f1"])
                sink(path: "file://output/path", scheme: text())
              }

            assembly()
              {
                regexParser(args: ["f1"], res: ["f1", "g1"], decl: ["g1"], pattern: /.*/, groups: [0])

                group(by: ["f1"], sort: ["g1"])

                sum(args: ["g1"], res: ["f1", "sum"])
              }
          }
      }

    assertEquals("names", "cascade", cascade.getName())
    assertEquals("flows", 1, cascade.getFlows().size())
  }

  void testFlowBrief()
  {
    def builder = new CascadingBuilder();

    def cascade = builder.cascade("cascade")
      {
        flow("flow")
          {
            source(path: "input/path", delete: true, fields: ["f1"])

            regexParser(args: ["f1"], res: ["f1", "g1"], decl: ["g1"], pattern: /.*/, groups: [0])
            group(by: ["f1"], sort: ["g1"])
            sum(args: ["g1"], res: ["f1", "sum"])

            sink(path: "file://output/path", scheme: text())
          }
      }

    assertEquals("names", "cascade", cascade.getName())
    assertEquals("flows", 1, cascade.getFlows().size())
  }

  void testFlowVeryBrief()
  {
    def builder = new CascadingBuilder();

    def cascade = builder("cascade")
      {
        flow("path1")
          {
            source(path: "input/path", fields: ["f1"])

            regexParser(args: ["f1"], res: ["f1", "g1"], decl: ["g1"], pattern: /.*/, groups: [0])
            group(by: ["f1"], sort: ["g1"])
            sum(args: ["g1"], res: ["f1", "sum"])

            sink(path: "file://output/path", scheme: text())
          }

        flow("path2")
          {
            source(path: "input/path2", fields: ["f1"])

            regexParser(args: ["f1"], res: ["f1", "g1"], decl: ["g1"], pattern: /.*/, groups: [0])
            group(by: ["f1"], sort: ["g1"])
            sum(args: ["g1"], res: ["f1", "sum"])

            sink(path: "file://output/path2", scheme: text())
          }
      }

    assertEquals("names", "cascade", cascade.getName())
    assertEquals("flows", 2, cascade.getFlows().size())
  }
}