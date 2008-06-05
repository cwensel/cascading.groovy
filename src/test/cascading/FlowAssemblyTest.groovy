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

}