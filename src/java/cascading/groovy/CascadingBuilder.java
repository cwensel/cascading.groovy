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

package cascading.groovy;

import java.text.SimpleDateFormat;
import java.util.Formatter;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.groovy.factory.AssemblyFactory;
import cascading.groovy.factory.BaseHolder;
import cascading.groovy.factory.CascadeFactory;
import cascading.groovy.factory.EndPointFactory;
import cascading.groovy.factory.FlowFactory;
import cascading.groovy.factory.GroupFactory;
import cascading.groovy.factory.IdentityFactory;
import cascading.groovy.factory.OperationFactory;
import cascading.groovy.factory.OperatorFactory;
import cascading.groovy.factory.SchemeFactory;
import cascading.groovy.factory.TapFactory;
import cascading.groovy.factory.TapMapFactory;
import cascading.groovy.factory.TypeOperationFactory;
import cascading.groovy.factory.regex.RegexFilterFactory;
import cascading.groovy.factory.regex.RegexParserFactory;
import cascading.groovy.factory.regex.RegexReplaceFactory;
import cascading.groovy.factory.regex.RegexSplitGeneratorFactory;
import cascading.groovy.factory.regex.RegexSplitterFactory;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Last;
import cascading.operation.aggregator.Max;
import cascading.operation.aggregator.Min;
import cascading.operation.aggregator.Sum;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexReplace;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;
import cascading.operation.text.FieldFormatter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import groovy.lang.Closure;
import groovy.util.FactoryBuilderSupport;

/**
 * CascadingBuilder is a Groovy <a href="http://groovy.codehaus.org/Builders">'builder'</a> extension.
 * <p/>
 * It supports the nested assembly of '{@link Tap} maps', {@link Pipe} assemblies, {@link Flow}s, and {@link Cascade}s.
 * Here is an example using the condensed format:
 * <pre>
 * def cascading = new Cascading()
 * def builder = cascading.builder();
 * <p/>
 * Cascade cascade = builder("cut cascade")
 *   {
 *     flow("cut")
 *       {
 *         source(inputFileApache)
 * <p/>
 *         cut(/\s+/, results: [1])
 *         group([0])
 * <p/>
 *         sink(outputPath + "cut-sort", delete: true)
 *       }
 *   }
 * <p/>
 * cascade.complete()
 * </pre>
 * Here is the same function in its full form:
 * <pre>
 *  def builder = new CascadingBuilder();
 * <p/>
 *  Cascade cascade = builder("cut cascade")
 *    {
 *      flow("cut flow")
 *        {
 *          map
 *          {
 *            source("cut")
 *              {
 *                lfs(inputFileApache)
 *                  {
 *                    text(["line"])
 *                  }
 *              }
 * <p/>
 *            sink("cut")
 *              {
 *                hfs(outputPath + "cut-sort-full", delete: true)
 *                  {
 *                    text()
 *                  }
 *              }
 *          }
 * <p/>
 *          assembly(name: "cut")
 *            {
 *              eachTuple(args: ["line"], results: [1])
 *                {
 *                  regexSplitter(/\s+/)
 *                }
 * <p/>
 *              group([0])
 * <p/>
 *              everyGroup(args: [0], results: ALL)
 *                {
 *                  count()
 *                }
 *            }
 *        }
 *    }
 * <p/>
 *  cascade.complete()
 * </pre>
 * This last form is necessary in order to support complex paths within and between flows.
 * <p/>
 * Additionally, within the eachTuple and everyGroup closure, user custom classes can be specified.
 * <pre>
 *  eachTuple(args: ["f1"], results: ["f1", "g1"])
 *    {
 *      operation(new RegexParser(new Fields("g1"), ".*", [0, 1] as Integer[]));
 *    }
 * </pre>
 * <p/>
 * <p/>
 * List of builder widgets:
 * <p/>
 * Core components:
 * <ul>
 * <li>cascade - Create a new Cascade. Expects 'name'.</li>
 * <li>flow - Create a new Flow. Expects 'name'.</li>
 * </ul>
 * Pipe assembly support:
 * <ul>
 * <li>assembly - Create a pipe assembly for inclusion in a Flow. Expects 'name'.</li>
 * <li>branch - Create a new join or split path in an assembly. Expects 'name'.</li>
 * <li>eachTuple - Create a new Each Operator. Accepts nested Function or Filter Operations. Expects 'arguments' ('args') and 'results' ('res'),
 * where the values are arrays. Optionally 'argumentFields' and 'resultFields' may be given, which are expected to be {@link Fields} instances.</li>
 * <li>everyGroup - Create a new Every Operator. Accepts nested Aggregator Operations. Expects same arguments as eachTuple.</li>
 * <li>operation - A child to eachTuple or everyGroup allowing for user Operation classes to be included in the assembly.</li>
 * </ul>
 * Tap and Scheme support:
 * <ul>
 * <li>map - Optional parent for source and sink.</li>
 * <li>source and sink - Create a new source Tap. Expects 'name' and optionally child arguments.</li>
 * <li>hfs and lfs - Creates an Hfs/Lfs Tap. Expects 'path' and optionaly 'delete' if resource should be deleted on exec.</li>
 * <li>text - Create a TextLine scheme, with default source field 'line'. Optionally accepts 'fields'.</li>
 * <li>sequence - Create a SequenceFile scheme. Expects 'fields'.</li>
 * <li>tap - Optional child to sink or source that allows for user Tap classes. Expects 'name'.</li>
 * </ul>
 * Group and Join support:
 * <ul>
 * <li>group - Create a new GroupBy. Accepts 'groupBy' and 'sortBy' fields.</li>
 * <li>join - Create a new CoGroup. Accepts 'groupBy' and 'declared' fields.</li>
 * </ul>
 * Functions and Filters (formal/alias). All Functions may take the argument 'declared' to override their default
 * declaredFields value:
 * <ul>
 * <li>debug - Creates a {@link Debug} Operation that simply prints out each Tuple to stdout.</li>
 * <li>copy - Creates a {@link Identity}. Passes incoming arguments as results.</li>
 * <li>coerce - Creates a {@link Identity}. Coerces incoming arguments to the given types in the 'types' argument.</li>
 * <li>regexParser - Creates a {@link RegexParser}. Expects regex 'pattern' and an int array of regex 'groups'</li>
 * <li>regexReplace/replace/replaceAll/replaceFirst - Creates a {@link RegexReplace}. Expects a regex 'pattern',
 * 'replacement' and optionally a boolean 'replaceAll'.</li>
 * <li>regexFilter/filter - Creates a {@link RegexFilter}. Expects regex 'pattern'.</li>
 * <li>regexSplitter/cut - Creates a {@link RegexSplitter}. Expects regex 'pattern'</li>
 * <li>regexSplitGenerator/tokenize - Creates a {@link RegexSplitGenerator}. Expects regex 'pattern'</li>
 * <li>dateFormatter - Creates a {@link DateFormatter}. Expects a {@link SimpleDateFormat} 'format'.</li>
 * <li>dateParser - Creates a {@link DateParser}. Expects a {@link SimpleDateFormat} 'format'.</li>
 * <li>fieldFormatter - Creates a {@link FieldFormatter}. Expects a {@link Formatter} 'format'.</li>
 * <li>fieldJointer - Creates a {@link FieldJoiner}. Expects a value 'delimiter' string.</li>
 * </ul>
 * Aggregators:
 * <ul>
 * <li>sum - </li>
 * <li>count - </li>
 * <li>first - </li>
 * <li>last - </li>
 * <li>min - </li>
 * <li>max - </li>
 * <li>avg - </li>
 * </ul>
 * <p/>
 * <p/>
 */
public class CascadingBuilder extends FactoryBuilderSupport
  {
  public static final Fields UNKNOWN = Fields.UNKNOWN;
  public static final Fields ALL = Fields.ALL;
  public static final Fields KEYS = Fields.KEYS;
  public static final Fields VALUES = Fields.VALUES;
  public static final Fields ARGS = Fields.ARGS;
  public static final Fields RESULTS = Fields.RESULTS;
  public static final Fields FIRST = Fields.FIRST;
  public static final Fields LAST = Fields.LAST;

  public CascadingBuilder()
    {
    registerFactories();
    }

  public CascadingBuilder( Closure closure )
    {
    super( closure );
    registerFactories();
    }

  @Override
  protected void setParent( Object parent, Object child )
    {
    if( parent instanceof BaseHolder )
      ( (BaseHolder) parent ).setChild( child );

    super.setParent( parent, child );
    }

  @Override
  protected void nodeCompleted( Object parent, Object node )
    {
    // should modify parents in node createInstance
    if( node instanceof BaseHolder )
      ( (BaseHolder) node ).handleParent( parent );

    super.nodeCompleted( parent, node );
    }

  @Override
  protected Object postNodeCompletion( Object parent, Object node )
    {
    if( node instanceof FlowFactory.FlowHolder )
      node = ( (FlowFactory.FlowHolder) node ).connectFlow();
    else if( node instanceof CascadeFactory.CascadeHolder )
      node = ( (CascadeFactory.CascadeHolder) node ).connectCascade();

    return super.postNodeCompletion( parent, node );
    }

  protected void registerFactories()
    {

    // Flow
    registerFactory( "call", new CascadeFactory( "default" ) );
    registerFactory( "cascade", new CascadeFactory() );
    registerFactory( "flow", new FlowFactory() );

    // Assembly
    registerFactory( "assembly", new AssemblyFactory() );
    registerFactory( "branch", new AssemblyFactory() );
    registerFactory( "eachTuple", new OperatorFactory() );
    registerFactory( "everyGroup", new OperatorFactory() );
    registerFactory( "operation", new OperationFactory() );

    registerFactory( "group", new GroupFactory() );
    registerFactory( "join", new GroupFactory() );

    //   operations
    registerFactory( "debug", new TypeOperationFactory( Debug.class ) );

    registerFactory( "copy", new IdentityFactory() );
    registerFactory( "project", new IdentityFactory() );
    registerFactory( "coerce", new IdentityFactory() );

    registerFactory( "regexParser", new RegexParserFactory() );
    registerFactory( "regexReplace", new RegexReplaceFactory() );
    registerFactory( "regexFilter", new RegexFilterFactory() );
    registerFactory( "regexSplitter", new RegexSplitterFactory() );
    registerFactory( "regexSplitGenerator", new RegexSplitGeneratorFactory() );

    registerFactory( "filter", new RegexFilterFactory() );
    registerFactory( "cut", new RegexSplitterFactory() );
    registerFactory( "tokenize", new RegexSplitGeneratorFactory() );
    registerFactory( "replace", new RegexReplaceFactory() );
    registerFactory( "replaceFirst", new RegexReplaceFactory( false ) );
    registerFactory( "replaceAll", new RegexReplaceFactory( true ) );

    registerFactory( "sum", new TypeOperationFactory( Sum.class ) );
    registerFactory( "count", new TypeOperationFactory( Count.class ) );
    registerFactory( "first", new TypeOperationFactory( First.class ) );
    registerFactory( "last", new TypeOperationFactory( Last.class ) );
    registerFactory( "min", new TypeOperationFactory( Min.class ) );
    registerFactory( "max", new TypeOperationFactory( Max.class ) );
    registerFactory( "avg", new TypeOperationFactory( Average.class ) );

    registerFactory( "dateFormatter", new TypeOperationFactory( DateFormatter.class, "format" ) );
    registerFactory( "dateParser", new TypeOperationFactory( DateParser.class, "format" ) );
    registerFactory( "fieldFormatter", new TypeOperationFactory( FieldFormatter.class, "format" ) );
    registerFactory( "fieldJoiner", new TypeOperationFactory( FieldJoiner.class, "delimiter" ) );

    // TapMap
    registerFactory( "map", new TapMapFactory() );
    registerFactory( "source", new EndPointFactory() );
    registerFactory( "sink", new EndPointFactory() );
    registerFactory( "tap", new TapFactory() );

    registerFactory( "hfs", new TapFactory() );
    registerFactory( "lfs", new TapFactory() );

    registerFactory( "text", new SchemeFactory() );
    registerFactory( "sequence", new SchemeFactory() );
    }

  }
