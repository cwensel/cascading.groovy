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
    return createFields( (ArrayList) findRemove( attributes, "declared", "decl" ) );
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
      types[ i ] = args[ i ].getClass();

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
  }
