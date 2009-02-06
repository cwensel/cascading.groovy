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

import java.util.ArrayList;
import java.util.Map;

import cascading.groovy.factory.OperationFactory;
import cascading.operation.Operation;
import cascading.operation.assertion.AssertEquals;
import cascading.tuple.Fields;

/**
 *
 */
public class AssertEqualsFactory extends OperationFactory
  {
  protected Comparable[] getValues( Object value, Map attributes )
    {
    Object values = attributes.remove( "values" );

    if( values == null )
      values = value;

    if( values == null )
      throw new RuntimeException( "values value is required" );

    if( !( values instanceof ArrayList ) )
      throw new RuntimeException( "values must be a list" );

    ArrayList list = (ArrayList) values;

    return (Comparable[]) list.toArray( new Comparable[list.size()] );
    }

  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    Comparable[] values = getValues( value, attributes );

    return (Operation) makeInstance( AssertEquals.class, null, (Object[]) values );
    }

  }