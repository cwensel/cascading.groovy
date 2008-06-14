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
