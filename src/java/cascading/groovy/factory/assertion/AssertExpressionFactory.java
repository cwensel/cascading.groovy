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