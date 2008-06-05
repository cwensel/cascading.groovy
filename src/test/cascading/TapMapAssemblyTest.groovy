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

    assertTrue("not delete", map.sources[ "path1" ].deleteOnSinkInit);
    assertTrue("is delete", !map.sinks[ "path1" ].deleteOnSinkInit);

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
      };

    print map;


    assertEquals("sources", 3, map.sources.size());
    assertEquals("sinks", 3, map.sinks.size());

    assertTrue("not multi", map.sources[ "path1" ] instanceof MultiTap);
    assertTrue("not hfs", map.sinks[ "path1" ] instanceof Hfs);

    assertTrue("not sequence", map.sources[ "path1" ].scheme instanceof SequenceFile);
    assertTrue("not text", map.sinks[ "path1" ].scheme instanceof TextLine);

    assertTrue("not delete", map.sources[ "path1" ].deleteOnSinkInit);
    assertTrue("is delete", !map.sinks[ "path1" ].deleteOnSinkInit);

  }

}