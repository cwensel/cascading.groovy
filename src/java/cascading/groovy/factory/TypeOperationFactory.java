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

package cascading.groovy.factory;

import java.util.Map;

import cascading.operation.Operation;
import cascading.tuple.Fields;

/**
 *
 */
public class TypeOperationFactory extends OperationFactory
  {
  private Class<? extends Operation> operationType;
  private String requiredField = null;

  public TypeOperationFactory( Class<? extends Operation> operationType, String requiredField )
    {
    this.operationType = operationType;
    this.requiredField = requiredField;
    }

  public TypeOperationFactory( Class<? extends Operation> operationType )
    {
    this.operationType = operationType;
    }

  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    String argument = null;

    if( requiredField != null )
      {
      argument = (String) attributes.remove( requiredField );

      if( argument == null )
        argument = (String) value;

      if( argument == null )
        throw new RuntimeException( String.format( "%s value is required", requiredField ) );
      }

    return (Operation) makeInstance( operationType, getDeclaredFields( attributes ), argument );
    }
  }