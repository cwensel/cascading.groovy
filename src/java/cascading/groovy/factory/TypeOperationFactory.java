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

    return (Operation) makeInstance( operationType, declaredFields, argument );
    }
  }