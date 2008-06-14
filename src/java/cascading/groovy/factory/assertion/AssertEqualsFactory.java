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
import cascading.operation.assertion.AssertEquals;
import cascading.tuple.Fields;
import groovyjarjarantlr.collections.List;

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

    if( !( values instanceof List ) )
      throw new RuntimeException( "values must be a list" );

    ArrayList list = (ArrayList) values;

    return (Comparable[]) list.toArray( new Comparable[list.size()] );
    }

  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    Comparable[] values = getValues( value, attributes );

    return (Operation) makeInstance( AssertEquals.class, null, values );
    }

  }