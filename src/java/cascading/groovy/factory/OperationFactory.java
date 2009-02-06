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

import java.util.Map;

import cascading.operation.Aggregator;
import cascading.operation.GroupAssertion;
import cascading.operation.Operation;
import cascading.tuple.Fields;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class OperationFactory extends OperatorFactory
  {
  @Override
  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    if( type != null && type.equals( "operation" ) )
      return value;

    Fields declaredFields = getDeclaredFields( attributes );

    Operation result = makeOperation( value, attributes, declaredFields );

    return returnInstance( builder, value, attributes, result );
    }

  protected Object returnInstance( FactoryBuilderSupport builder, Object value, Map attributes, Operation result ) throws InstantiationException, IllegalAccessException
    {
    if( builder.getParentFactory() instanceof OperatorFactory )
      return result;

    // supports abbreviated syntax
    String type = result instanceof Aggregator || result instanceof GroupAssertion ? "everyGroup" : "eachTuple";

    OperatorHolder operator = (OperatorHolder) super.newInstance( builder, type, value, attributes );

    operator.setOperation( (Operation) result );

    return operator;
    }

  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    return null;
    }

  }
