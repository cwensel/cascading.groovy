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
import cascading.operation.assertion.AssertExpression;
import cascading.tuple.Fields;

/**
 *
 */
public class AssertExpressionFactory extends OperationFactory
  {
  protected String getExpression( Object value, Map attributes )
    {
    String expression = (String) attributes.remove( "expression" );

    if( expression == null )
      expression = (String) value;

    if( expression == null )
      throw new RuntimeException( "expression value is required" );

    return expression;
    }

  protected Class[] getTypes( Object value, Map attributes )
    {
    Object values = attributes.remove( "types" );

    if( values == null )
      throw new RuntimeException( "types value is required" );

    if( !( values instanceof ArrayList ) )
      throw new RuntimeException( "types must be a list" );

    ArrayList list = (ArrayList) values;

    return (Class[]) list.toArray( new Class[list.size()] );
    }

  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    String expression = getExpression( value, attributes );

    Class[] types = getTypes( value, attributes );

    return (Operation) makeInstance( AssertExpression.class, null, expression, (Object) types );
    }

  }