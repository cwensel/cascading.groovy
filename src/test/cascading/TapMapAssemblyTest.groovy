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
import cascading.scheme.SequenceFile
import cascading.scheme.TextLine
import cascading.tap.Hfs
import cascading.tap.Lfs
import cascading.tap.MultiTap
import cascading.tuple.Fields

class TapMapAssemblyTest extends GroovyTestCase
{

  public TapMapAssemblyTest()
  {
  }

  void testVerbose()
  {
    def builder = new CascadingBuilder();

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
      };

    print map;

    assertEquals("sources", 1, map.sources.size());
    assertEquals("sinks", 1, map.sinks.size());

    assertTrue("not hfs", map.sources[ "path" ] instanceof Hfs);
    assertTrue("not lfs", map.sinks[ "path" ] instanceof Lfs);
  }

  void testVerbosePaths()
  {
    def builder = new CascadingBuilder();

    def map = builder.map()
      {
        source(name: "path")
          {
            hfs(["input/path1/", "input/path2/"], delete: true)
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
      };

    print map;


    assertEquals("sources", 1, map.sources.size());
    assertEquals("sinks", 1, map.sinks.size());

    assertTrue("not multi", map.sources[ "path" ] instanceof MultiTap);
    assertTrue("not lfs", map.sinks[ "path" ] instanceof Lfs);
  }

  void testVerbosePathsPath()
  {
    def builder = new CascadingBuilder();

    def map = builder.map()
      {
        source(name: "path")
          {
            hfs(path: ["input/path1/", "input/path2/"], delete: true)
              {
                sequence(["f1"])
              }
          }

        sink(name: "path")
          {
            lfs(path: "output/path", delete: true)
              {
                text()
              }
          }
      };

    print map;


    assertEquals("sources", 1, map.sources.size());
    assertEquals("sinks", 1, map.sinks.size());

    assertTrue("not multi", map.sources[ "path" ] instanceof MultiTap);
    assertTrue("not lfs", map.sinks[ "path" ] instanceof Lfs);
  }

  void testExplicit()
  {
    def builder = new CascadingBuilder();

    def map = builder.map()
      {
        source(name: "path")
          {
            tap(new Hfs(new SequenceFile(new Fields("f1")), "input/path", true))
          }

        sink(name: "path")
          {
            lfs("output/path", delete: true)
              {
                text()
              }
          }

        trap(name: "path")
          {
            lfs("output/path", delete: true)
              {
                text()
              }
          }
      };

    print map;

    assertEquals("sources", 1, map.sources.size());
    assertEquals("sinks", 1, map.sinks.size());

    assertTrue("not hfs", map.sources[ "path" ] instanceof Hfs);
    assertTrue("not hfs", map.sinks[ "path" ] instanceof Lfs);
  }


  void testBrief()
  {
    def builder = new CascadingBuilder();

    def map = builder.map()
      {
        source(name: "path1", path: "input/path", delete: true, fields: ["f1"])
        source(name: "path2", path: "input/path", delete: true, fields: ["f1"])
        source(name: "path3", path: "input/path", delete: true, fields: ["f1"])

        sink(name: "path1", path: "file://output/path", scheme: text())
        sink(name: "path2", path: "file://output/path", scheme: text())
        sink(name: "path3", path: "file://output/path", scheme: text())
      };

    print map;


    assertEquals("sources", 3, map.sources.size());
    assertEquals("sinks", 3, map.sinks.size());

    assertTrue("not hfs", map.sources[ "path1" ] instanceof Hfs);
    assertTrue("not hfs", map.sinks[ "path1" ] instanceof Hfs);

    assertTrue("not sequence", map.sources[ "path1" ].scheme instanceof SequenceFile);
    assertTrue("not text", map.sinks[ "path1" ].scheme instanceof TextLine);

    assertTrue("not delete", map.sources[ "path1" ].replace);
    assertTrue("is delete", !map.sinks[ "path1" ].replace);

  }

  void testBriefPaths()
  {
    def builder = new CascadingBuilder();

    def map = builder.map()
      {
        source(name: "path1", path: ["input/path1", "input/path2"], delete: true, fields: ["f1"])
        source(name: "path2", path: "input/path", delete: true, fields: ["f1"])
        source(name: "path3", path: "input/path", delete: true, fields: ["f1"])

        sink(name: "path1", path: "file://output/path", scheme: text())
        sink(name: "path2", path: "file://output/path", scheme: text())
        sink(name: "path3", path: "file://output/path", scheme: text())

        trap(name: "path1", path: "file://output/path", scheme: text())

      };

    print map;


    assertEquals("sources", 3, map.sources.size());
    assertEquals("sinks", 3, map.sinks.size());

    assertTrue("not multi", map.sources[ "path1" ] instanceof MultiTap);
    assertTrue("not hfs", map.sinks[ "path1" ] instanceof Hfs);

    assertTrue("not sequence", map.sources[ "path1" ].scheme instanceof SequenceFile);
    assertTrue("not text", map.sinks[ "path1" ].scheme instanceof TextLine);

    assertTrue("is delete", !map.sources[ "path1" ].replace); // irrelevant, not a sink
    assertTrue("is delete", !map.sinks[ "path1" ].replace);

  }

}