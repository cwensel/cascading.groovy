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

  void testAssertAndTrap()
  {
    def builder = new CascadingBuilder();

    Flow flow = builder.flow("grep")
      {
        source(inputFileApache)

        filter(/.*88.*/)
        assertMatches(level: STRICT, pattern: /.*1111.*/) // guaranteed to fail

        sink(outputPath + "assert", delete: true)
        trap(outputPath + "assertTrap", delete: true)
      }

    flow.complete()

    verifySinks(flow, 0)
    verifyTraps(flow, 1)
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

  private def verifyTraps(Flow flow, int size, String match = ".*")
  {
    return flow.traps.values().each {tap ->

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