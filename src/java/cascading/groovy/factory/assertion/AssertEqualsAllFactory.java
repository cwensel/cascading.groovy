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

package cascading.groovy.factory.assertion;

import java.util.Map;

import cascading.groovy.factory.OperationFactory;
import cascading.operation.Operation;
import cascading.operation.assertion.AssertEquals;
import cascading.tuple.Fields;

/**
 *
 */
public class AssertEqualsAllFactory extends OperationFactory
  {
  protected Object getValues( Object value, Map attributes )
    {
    Object values = attributes.remove( "value" );

    if( values == null )
      values = value;

    if( values == null )
      throw new RuntimeException( "values value is required" );

    return values;
    }

  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    Object values = getValues( value, attributes );

    return (Operation) makeInstance( AssertEquals.class, null, values );
    }

  }