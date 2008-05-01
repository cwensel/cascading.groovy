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

package cascading

import cascading.flow.Flow
import cascading.groovy.CascadingBuilder

class FlowTest extends GroovyTestCase
{

  String inputFileApache = "build/test/data/apache.10.txt";
  String outputPath = "build/test/output/flow/";

  public FlowTest()
  {

  }

  void testCopy()
  {
    def builder = new CascadingBuilder();

    Flow flow = builder.flow("copy")
      {
        source(inputFileApache)

        copy()

        sink(outputPath + "copy", delete: true)
      }

    flow.complete()

    verifySinks(flow, 10)
  }

  void testCut()
  {
    def builder = new CascadingBuilder();

    Flow flow = builder.flow("cut")
      {
        source(inputFileApache)

        cut(/\s+/, results: [1])

        sink(outputPath + "cut", delete: true)
      }

    flow.complete()

    verifySinks(flow, 10, /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)
  }

  void testCutSort()
  {
    def builder = new CascadingBuilder();

    Flow flow = builder.flow("cut")
      {
        source(inputFileApache)

        cut(/\s+/, results: [1])
        group([0])

        sink(outputPath + "cut-sort", delete: true)
      }

    flow.complete()

    verifySinks(flow, 10, /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)
  }

  void testFilter()
  {
    def builder = new CascadingBuilder();

    Flow flow = builder.flow("grep")
      {
        source(inputFileApache)

        filter(/.*88.*/)

        sink(outputPath + "filter", delete: true)
      }

    flow.complete()

    verifySinks(flow, 1)
  }

  void testFilterGrouped()
  {
    def builder = new CascadingBuilder();

    Flow flow = builder.flow("grep")
      {
        source(inputFileApache)

        filter(/.*88.*/)
        group(["line"])

        sink(outputPath + "filter-grouped", delete: true)
      }

    flow.complete()

    verifySinks(flow, 1)
  }

  private def verifySinks(Flow flow, int size, String match = ".*")
  {
    return flow.sinks.values().each {tap ->

      println tap

      assertTrue(flow.tapPathExists(tap))

      def count = 0
      flow.openTapForRead(tap).each {tuple ->
        assertTrue(tuple.toString().matches(match));
        count++;
      }

      assertEquals(size, count)
    }
  }

}