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

import cascading.groovy.CascadingBuilder

class FlowAssemblyTest extends GroovyTestCase
{

  public FlowAssemblyTest()
  {

  }

  void testFlow()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly(name: "path")
      {
        eachTuple(args: ["f1"], res: ["f1", "g1"])
          {
            regexParser(decl: ["g1"], pattern: /.*/, groups: [0, 1])
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

    def flow = builder.flow(name: "flow", map: map, assembly: assembly);

    assertEquals("sources", 1, flow.sources.size());
    assertEquals("sinks", 1, flow.sinks.size());
  }

  void testFlowNested()
  {
    def builder = new CascadingBuilder();

    def flow = builder.flow("flow")
      {
        assembly(name: "path")
          {
            eachTuple(args: ["f1"], res: ["f1", "g1"])
              {
                regexParser(decl: ["g1"], pattern: /.*/, groups: [0, 1])
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

    assertEquals("sources", 1, flow.sources.size());
    assertEquals("sinks", 1, flow.sinks.size());
  }

  void testFlowAbbreviated()
  {
    def builder = new CascadingBuilder();

    def flow = builder.flow("flow")
      {
        map()
          {
            source(path: "input/path", delete: true, fields: ["f1"])
            sink(path: "file://output/path", scheme: text())
          }

        assembly()
          {
            regexParser(args: ["f1"], res: ["f1", "g1"], decl: ["g1"], pattern: /.*/, groups: [0, 1])

            group(by: ["f1"], sort: ["g1"])

            sum(args: ["g1"], res: ["f1", "sum"])
          }
      }

    assertEquals("sources", 1, flow.sources.size());
    assertEquals("sinks", 1, flow.sinks.size());
  }

  void testFlowBrief()
  {
    def builder = new CascadingBuilder();

    def flow = builder.flow("flow")
      {
        source(path: "input/path", delete: true, fields: ["f1"])

        regexParser(args: ["f1"], res: ["f1", "g1"], decl: ["g1"], pattern: /.*/, groups: [0, 1])
        group(by: ["f1"], sort: ["g1"])
        sum(args: ["g1"], res: ["f1", "sum"])

        sink(path: "file://output/path", scheme: text())
      }

    assertEquals("sources", 1, flow.sources.size());
    assertEquals("sinks", 1, flow.sinks.size());
  }


  void testFlowBriefWithAssertions()
  {
    def builder = new CascadingBuilder();

    def flow = builder.flow("flow")
      {
        source(path: "input/path", delete: true, fields: ["f1"])

        regexParser(args: ["f1"], res: ["f1", "g1"], decl: ["g1"], pattern: /.*/, groups: [0, 1])
        assertNotNullValues(args: ["f1"], level: STRICT)
        group(by: ["f1"], sort: ["g1"])
        sum(args: ["g1"], res: ["f1", "sum"])

        sink(path: "file://output/path", scheme: text())
      }

    assertEquals("sources", 1, flow.sources.size());
    assertEquals("sinks", 1, flow.sinks.size());
  }
}