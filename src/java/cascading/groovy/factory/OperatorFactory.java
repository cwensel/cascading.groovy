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

import java.util.Arrays;
import java.util.Map;

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

      if( assembly.getPreviousPipe() != null )
        pipe = new Pipe( name, assembly.getPreviousPipe() ); // type the split
      else if( assembly.getLastChild() != null )
        pipe = (Pipe) assembly.getLastChild();

      if( arguments != null )
        argumentFields = new Fields( arguments );

      if( results != null )
        resultFields = new Fields( results );

      if( getType().equalsIgnoreCase( "eachTuple" ) )
        return makePipe( Each.class, name, pipe, argumentFields, getOperation(), resultFields );

      if( getType().equalsIgnoreCase( "everyGroup" ) )
        return makePipe( Every.class, name, pipe, argumentFields, getOperation(), resultFields );

      throw new RuntimeException( "type: " + getType() + " was not found" );
      }

    public String toString()
      {
      return "OperatorHolder{" + "type='" + type + '\'' + ", arguments=" + ( arguments == null ? null : Arrays.asList( arguments ) ) + ", results=" + ( results == null ? null : Arrays.asList( results ) ) + '}';
      }
    }
  }
