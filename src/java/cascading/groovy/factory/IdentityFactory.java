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

import java.util.ArrayList;
import java.util.Map;

import cascading.operation.Identity;
import cascading.operation.Operation;
import cascading.tuple.Fields;

/**
 *
 */
public class IdentityFactory extends OperationFactory
  {
  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    ArrayList typesList = (ArrayList) attributes.remove( "types" );

    return (Operation) makeInstance( Identity.class, declaredFields, (Object) createClassArray( typesList ) );
    }

  protected Class[] createClassArray( ArrayList typesList )
    {
    if( typesList == null )
      return null;

    Class[] results = new Class[typesList.size()];

    for( int i = 0; i < typesList.size(); i++ )
      results[ i ] = (Class) typesList.get( i );

    return results;
    }


  }