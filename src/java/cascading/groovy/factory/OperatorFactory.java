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

import java.util.Arrays;
import java.util.Map;

import cascading.operation.AssertionLevel;
import cascading.operation.Operation;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class OperatorFactory extends BaseFactory
  {
  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    rename( attributes, "arguments", "args" );
    rename( attributes, "results", "res" );

    rename( attributes, Fields.class, "argumentFields", "arguments" );
    rename( attributes, Fields.class, "resultFields", "results" );

    rename( attributes, "assertionLevel", "level" );

    return new OperatorHolder( (String) type );
    }

  /**
   *
   */
  public static class OperatorHolder extends PipeHolder
    {
    Comparable[] arguments;
    Comparable[] results;
    Fields argumentFields;
    Fields resultFields;
    AssertionLevel assertionLevel;

    public OperatorHolder( String type )
      {
      super( type );
      }

    public OperatorHolder( String type, Operation operation )
      {
      super( type, operation );
      }

    public Object createInstance( Object parent )
      {
      if( parent instanceof FlowFactory.FlowHolder )
        parent = ( (FlowFactory.FlowHolder) parent ).assembly;

      AssemblyFactory.Assembly assembly = (AssemblyFactory.Assembly) parent;

      String name = assembly.name;
      Pipe pipe = null;

      if( assembly.getLastChild() != null )
        pipe = (Pipe) assembly.getLastChild();
      else if( assembly.getPreviousPipe() != null )
        pipe = new Pipe( name, assembly.getPreviousPipe() ); // type the split

      if( arguments != null )
        argumentFields = new Fields( arguments );

      if( results != null )
        resultFields = new Fields( results );

      if( getType().equalsIgnoreCase( "eachTuple" ) )
        return makePipe( Each.class, name, pipe, argumentFields, assertionLevel, getOperation(), resultFields );

      if( getType().equalsIgnoreCase( "everyGroup" ) )
        return makePipe( Every.class, name, pipe, argumentFields, assertionLevel, getOperation(), resultFields );

      throw new RuntimeException( "type: " + getType() + " was not found" );
      }

    public String toString()
      {
      return "OperatorHolder{" + "type='" + type + '\'' + ", arguments=" + ( arguments == null ? null : Arrays.asList( arguments ) ) + ", results=" + ( results == null ? null : Arrays.asList( results ) ) + '}';
      }
    }
  }
