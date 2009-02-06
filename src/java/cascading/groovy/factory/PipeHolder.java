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
