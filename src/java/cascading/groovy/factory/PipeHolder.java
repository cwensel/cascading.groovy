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

import cascading.operation.Aggregator;
import cascading.operation.Assertion;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Operation;
import cascading.pipe.Pipe;
import cascading.util.Util;

/**
 *
 */
public abstract class PipeHolder extends BaseHolder
  {
  Operation operation;

  protected PipeHolder( String type )
    {
    this.type = type;
    }

  protected PipeHolder( String name, Operation operation )
    {
    this.type = name;
    this.operation = operation;
    }

  public Object getOperation()
    {
    return operation;
    }

  public void setOperation( Operation operation )
    {
    this.operation = operation;
    }

  public void setChild( Object child )
    {
    setOperation( (Operation) child );
    }

  public abstract Object createInstance( Object parent );

  public void handleParent( Object parent )
    {
    Pipe pipe = (Pipe) createInstance( parent );

    if( parent instanceof AssemblyFactory.Assembly )
      {
      ( (AssemblyFactory.Assembly) parent ).addTail( pipe );
      ( (AssemblyFactory.Assembly) parent ).setLastChild( pipe );
      }
    }

  protected Pipe makePipe( Class type, String name, Pipe pipe, Object... objects )
    {
    ArrayList list = new ArrayList();

    if( pipe == null )
      list.add( name );

    list.add( pipe );

    return makePipe( type, list, objects );
    }

  protected Pipe makePipe( Class type, Object... objects )
    {
    ArrayList list = new ArrayList();

    return makePipe( type, list, objects );
    }

  private Pipe makePipe( Class type, ArrayList list, Object... objects )
    {
    Collections.addAll( list, objects );
    Util.removeAllNulls( list );

    Object[] args = list.toArray();
    Class[] types = new Class[args.length];

    for( int i = 0; i < args.length; i++ )
      {
      types[ i ] = args[ i ].getClass();

      if( Pipe.class.isAssignableFrom( types[ i ] ) )
        types[ i ] = Pipe.class;
      else if( Function.class.isAssignableFrom( types[ i ] ) )
        types[ i ] = Function.class;
      else if( Filter.class.isAssignableFrom( types[ i ] ) )
        types[ i ] = Filter.class;
      else if( Aggregator.class.isAssignableFrom( types[ i ] ) )
        types[ i ] = Aggregator.class;
      else if( Assertion.class.isAssignableFrom( types[ i ] ) )
        types[ i ] = Assertion.class;
      else if( Boolean.class.isAssignableFrom( types[ i ] ) )
        types[ i ] = Boolean.TYPE;
      }

    try
      {
      return (Pipe) type.getConstructor( types ).newInstance( args );
      }
    catch( Exception exception )
      {
      throw new RuntimeException( exception );
      }
    }
  }
