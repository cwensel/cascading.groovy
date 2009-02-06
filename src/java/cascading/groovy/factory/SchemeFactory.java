/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.groovy.factory;

import java.util.List;
import java.util.Map;

import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tuple.Fields;
import groovy.util.FactoryBuilderSupport;


/**
 *
 */
public class SchemeFactory extends BaseFactory
  {
  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    if( type.equals( "text" ) )
      return createTextLine( value, attributes );
    else if( type.equals( "sequence" ) )
      return createSequenceFile( value, attributes );

    throw new RuntimeException( "unknown scheme type: " + type );
    }

  protected Scheme createTextLine( Object value, Map attributes )
    {
    Fields fields = createFields( (List) value );

    if( fields == null )
      fields = createFields( (List) findRemove( attributes, "fields" ) );

    if( fields != null )
      return new TextLine( fields );
    else
      return createDefaultTextLine();
    }

  static TextLine createDefaultTextLine()
    {
    return new TextLine( new Fields( "line" ) );
    }

  protected Scheme createSequenceFile( Object value, Map attributes )
    {
    Fields fields = createFields( (List) value );

    if( fields == null )
      fields = createFields( (List) findRemove( attributes, "fields" ) );

    if( fields != null )
      return new SequenceFile( fields );
    else
      throw new RuntimeException( "fields are required" );
    }
  }
