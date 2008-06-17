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

package cascading.groovy;

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
import cascading.groovy.factory.assertion.AssertEqualsAllFactory;
import cascading.groovy.factory.assertion.AssertEqualsFactory;
import cascading.groovy.factory.assertion.AssertExpressionFactory;
import cascading.groovy.factory.assertion.AssertMatchesFactory;
import cascading.groovy.factory.regex.RegexFilterFactory;
import cascading.groovy.factory.regex.RegexParserFactory;
import cascading.groovy.factory.regex.RegexReplaceFactory;
import cascading.groovy.factory.regex.RegexSplitGeneratorFactory;
import cascading.groovy.factory.regex.RegexSplitterFactory;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Last;
import cascading.operation.aggregator.Max;
import cascading.operation.aggregator.Min;
import cascading.operation.aggregator.Sum;
import cascading.operation.assertion.AssertMatches;
import cascading.operation.assertion.AssertMatchesAll;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.assertion.AssertNull;
import cascading.operation.assertion.AssertSizeEquals;
import cascading.operation.assertion.AssertSizeLessThan;
import cascading.operation.assertion.AssertSizeMoreThan;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;
import cascading.operation.text.FieldFormatter;
import cascading.operation.text.FieldJoiner;
import cascading.tuple.Fields;
import groovy.lang.Closure;
import groovy.util.FactoryBuilderSupport;

/** CascadingBuilder is a Groovy <a href="http://groovy.codehaus.org/Builders">'builder'</a> extension. */
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

  public static final AssertionLevel STRICT = AssertionLevel.STRICT;
  public static final AssertionLevel VALID = AssertionLevel.VALID;
  public static final AssertionLevel NONE = AssertionLevel.NONE;

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
    registerFactory( "sort", new GroupFactory() );
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

    // assertions
    registerFactory( "assertNullValues", new TypeOperationFactory( AssertNull.class ) );
    registerFactory( "assertNotNullValues", new TypeOperationFactory( AssertNotNull.class ) );
    registerFactory( "assertSizeEquals", new TypeOperationFactory( AssertSizeEquals.class, "size" ) );
    registerFactory( "assertSizeLessThan", new TypeOperationFactory( AssertSizeLessThan.class, "size" ) );
    registerFactory( "assertSizeMoreThan", new TypeOperationFactory( AssertSizeMoreThan.class, "size" ) );
    registerFactory( "assertMatches", new AssertMatchesFactory( AssertMatches.class ) );
    registerFactory( "assertMatchesAll", new AssertMatchesFactory( AssertMatchesAll.class ) );
    registerFactory( "assertEqualsValues", new AssertEqualsFactory() ); // expects values
    registerFactory( "assertEqualsAll", new AssertEqualsAllFactory() ); // expects value
    registerFactory( "assertExpression", new AssertExpressionFactory() ); // expects value

    // TapMap
    registerFactory( "map", new TapMapFactory() );
    registerFactory( "source", new EndPointFactory() );
    registerFactory( "sink", new EndPointFactory() );
    registerFactory( "trap", new EndPointFactory() );
    registerFactory( "tap", new TapFactory() );

    registerFactory( "hfs", new TapFactory() );
    registerFactory( "lfs", new TapFactory() );

    registerFactory( "text", new SchemeFactory() );
    registerFactory( "sequence", new SchemeFactory() );
    }

  }
