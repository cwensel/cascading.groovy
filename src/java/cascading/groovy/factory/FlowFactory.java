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

import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class FlowFactory extends BaseFactory
  {
  public FlowFactory()
    {
    }

  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    return new FlowHolder( (String) type, (String) value );
    }

  public static class FlowHolder extends BaseHolder
    {
    String name;
    boolean skipIfSinkExists = false;
    AssemblyFactory.Assembly assembly = new AssemblyFactory.Assembly();
    TapMap map = new TapMap();

    Flow flow;

    public FlowHolder( String type, String name )
      {
      super( type );
      this.name = name;
      this.assembly.name = name;
      }

    public String getName()
      {
      return name;
      }

    public void setAssembly( AssemblyFactory.Assembly assembly )
      {
      this.assembly = assembly;
      }

    public void setMap( TapMap map )
      {
      this.map = map;
      }

    public void setChild( Object child )
      {
      if( child instanceof AssemblyFactory.Assembly )
        setAssembly( (AssemblyFactory.Assembly) child );
      else if( child instanceof TapMap )
        setMap( (TapMap) child );
      else if( child instanceof EndPointFactory.EndPointHolder )
        ( (EndPointFactory.EndPointHolder) child ).handleParent( map );
      else if( child instanceof PipeHolder )
        ( (PipeHolder) child ).handleParent( assembly );
//      else
//        throw new RuntimeException( "unknown child type: " + child );
      }

    public void handleParent( Object parent )
      {
      //To change body of implemented methods use File | Settings | File Templates.
      }

    public Flow connectFlow()
      {
      if( flow != null )
        return flow;

      FlowConnector flowConnector = new FlowConnector();

      if( map.getSources().size() == 1 && map.getSinks().size() == 1 && assembly.getTails().size() == 1 )
        flow = flowConnector.connect( name, map.getSource(), map.getSink(), assembly.getTail() );
      else if( map.getSources().size() == 1 )
        flow = flowConnector.connect( name, map.getSource(), map.getSinks(), assembly.getTailsArray() );
      else if( map.getSinks().size() == 1 )
        flow = flowConnector.connect( name, map.getSources(), map.getSink(), assembly.getTail() );
      else
        flow = flowConnector.connect( name, map.getSources(), map.getSinks(), assembly.getTailsArray() );

      flow.setSkipIfSinkExists( skipIfSinkExists );

      return flow;
      }
    }

  }