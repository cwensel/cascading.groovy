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