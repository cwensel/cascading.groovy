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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.util.Util;
import groovy.util.AbstractFactory;

/**
 *
 */
public abstract class BaseFactory extends AbstractFactory
  {

  protected Fields getDeclaredFields( Map attributes )
    {
    Object fields = findRemove( attributes, "declared", "decl" );

    if( fields instanceof ArrayList )
      return createFields( (ArrayList) fields );
    else
      return (Fields) fields;
    }

  protected Fields createFields( List fieldsList )
    {
    if( fieldsList == null )
      return null;

    return new Fields( (Comparable[]) fieldsList.toArray( new Comparable[fieldsList.size()] ) );
    }

  protected int[] createIntegerArray( ArrayList groupList )
    {
    if( groupList == null )
      return null;

    int[] results = new int[groupList.size()];

    for( int i = 0; i < groupList.size(); i++ )
      results[ i ] = (Integer) groupList.get( i );

    return results;
    }

  protected Object makeInstance( Class type, Fields declaredFields, Object... arguments )
    {
    if( arguments == null )
      arguments = new Object[0];

    ArrayList list = new ArrayList();

    list.add( declaredFields );
    Collections.addAll( list, arguments );

    Util.removeAllNulls( list );

    Object[] args = list.toArray();
    Class[] types = new Class[args.length];

    for( int i = 0; i < args.length; i++ )
      types[ i ] = makePrimitive( args[ i ].getClass() );

    try
      {
      return type.getConstructor( types ).newInstance( args );
      }
    catch( Exception exception )
      {
      throw new RuntimeException( exception );
      }
    }

  protected Object findRemove( Map attributes, String... args )
    {
    for( String arg : args )
      {
      Object result = attributes.remove( arg );

      if( result != null )
        return result;
      }

    return null;
    }

  protected void rename( Map attributes, Class type, String... args )
    {
    for( int i = 1; i < args.length; i++ )
      {
      if( type.isInstance( attributes.get( args[ i ] ) ) )
        rename( attributes, args );
      }
    }

  /**
   * First is the to rename, remaining are aliases.
   *
   * @param attributes
   * @param args
   */
  protected void rename( Map attributes, String... args )
    {
    if( attributes.containsKey( args[ 0 ] ) )
      return;

    for( int i = 1; i < args.length; i++ )
      {
      Object value = attributes.remove( args[ i ] );

      if( value == null )
        continue;

      attributes.put( args[ 0 ], value );

      break;
      }
    }

  protected Class makePrimitive( Class type )
    {
    if( type == Integer.class )
      return int.class;
    else if( type == Long.class )
      return long.class;
    else if( type == Double.class )
      return double.class;
    else if( type == Float.class )
      return float.class;
    else if( type == Short.class )
      return short.class;
    else if( type == Boolean.class )
      return boolean.class;

    return type;
    }
  }
