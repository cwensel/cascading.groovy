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
import cascading.operation.regex.RegexParser
import cascading.tuple.Fields

class PipeAssemblyTest extends GroovyTestCase
{

  public PipeAssemblyTest()
  {
  }

  void testChain()
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

    print assembly;

    assertEquals("heads", 1, assembly.heads.size())
    assertEquals("tails", 1, assembly.tails.size())
  }


  void testChainWildcards()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly(name: "path")
      {
        eachTuple(args: ALL, res: RESULTS)
          {
            regexParser(decl: ["g1"], pattern: /.*/, groups: [0, 1])
          }

        group(["f1"])

        everyGroup(args: ["g1"], res: VALUES)
          {
            sum()
          }
      }

    print assembly;

    assertEquals("heads", 1, assembly.heads.size())
    assertEquals("tails", 1, assembly.tails.size())
  }

  void testChainBrief()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly("path")
      {
        regexParser(args: ["f1"], res: ["f1", "g1"], decl: ["g1"], pattern: /.*/, groups: [0, 1])

        group(by: ["f1"], sort: ["g1"])

        sum(args: ["g1"], res: ["f1", "sum"])
      }

    print assembly;

    assertEquals("heads", 1, assembly.heads.size())
    assertEquals("tails", 1, assembly.tails.size())
  }

  void testChainExplicit()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly(name: "path")
      {
        eachTuple(args: ["f1"], res: ["f1", "g1"])
          {
            operation(new RegexParser(new Fields("g1"), ".*", [0, 1] as Integer[]));
          }

        group(["f1"])

        everyGroup(args: ["g1"], res: ["f1", "sum"])
          {
            sum()
          }
      }

    print assembly;

    assertEquals("heads", 1, assembly.heads.size())
    assertEquals("tails", 1, assembly.tails.size())
  }

  void testJoin()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly(name: "path")
      {
        branch("lhs")
          {
            eachTuple(args: ["f1"], results: ["f1", "g1"])
              {
                regexParser(declared: ["g1"], pattern: /.*/, groups: [0, 1])
              }
          }

        branch("rhs")
          {
            eachTuple(args: ["f1"], results: ["f1", "g2"])
              {
                regexParser(declared: ["g2"], pattern: /.*/, groups: [0, 1])
              }
          }

        join(lhs: ["f1"], rhs: ["f1"], declared: ["f1", "g1", "f2", "g2"])

        everyGroup(args: ["g1"], results: ["f1", "sum"])
          {
            sum()
          }
      }

    print assembly;

    assertEquals("heads", 2, assembly.heads.size())
    assertEquals("tails", 1, assembly.tails.size())
  }

  void testJoinBrief()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly(name: "path")
      {
        branch("lhs")
          {
            regexParser(args: ["f1"], results: ["f1", "g1"], declared: ["g1"], pattern: /.*/, groups: [0, 1])
          }

        branch("rhs")
          {
            regexParser(args: ["f1"], results: ["f1", "g2"], declared: ["g2"], pattern: /.*/, groups: [0, 1])
          }

        join(lhs: ["f1"], rhs: ["f1"], decl: ["f1", "g1", "f2", "g2"])

        sum(args: ["g1"], results: ["f1", "sum"])
      }

    print assembly;

    assertEquals("heads", 2, assembly.heads.size())
    assertEquals("tails", 1, assembly.tails.size())
  }

  void testSplit()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly(name: "path")
      {
        eachTuple(args: ["f0"], res: ["f1"])
          {
            regexParser(decl: ["f1"], pattern: /.*/, groups: [0, 1])
          }

        branch("lhs")
          {
            eachTuple(args: ["f1"], results: ["f1", "g1"])
              {
                regexParser(declared: ["g1"], pattern: /.*/, groups: [0, 1])
              }

            group(["f1"])

            everyGroup(args: ["g1"], results: ["f1", "sum"])
              {
                sum()
              }
          }

        branch("rhs")
          {
            eachTuple(args: ["f1"], results: ["f1", "g2"])
              {
                regexParser(declared: ["g2"], pattern: /.*/, groups: [0, 1])
              }

            group(["f1"])

            everyGroup(args: ["g2"], results: ["f1", "sum"])
              {
                sum()
              }
          }
      }

    print assembly;

    assertEquals("heads", 1, assembly.heads.size())
    assertEquals("tails", 2, assembly.tails.size())
  }

  void testSplitBrief()
  {
    def builder = new CascadingBuilder();

    def assembly = builder.assembly(name: "path")
      {
        regexParser(args: ["f0"], res: ["f1"], decl: ["f1"], pattern: /.*/, groups: [0, 1])

        branch("lhs")
          {
            regexParser(args: ["f1"], results: ["f1", "g1"], declared: ["g1"], pattern: /.*/, groups: [0, 1])

            group(["f1"])

            sum(args: ["g1"], results: ["f1", "sum"])
          }

        branch("rhs")
          {
            regexParser(args: ["f1"], results: ["f1", "g2"], declared: ["g2"], pattern: /.*/, groups: [0, 1])

            group(["f1"])

            sum(args: ["g2"], results: ["f1", "sum"])
          }
      }

    print assembly;

    assertEquals("heads", 1, assembly.heads.size())
    assertEquals("tails", 2, assembly.tails.size())
  }

}