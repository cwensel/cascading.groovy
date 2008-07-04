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
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.AssertionLevel;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class FlowFactory extends BaseFactory
  {
  private final Properties properties;

  public FlowFactory( Properties properties )
    {
    this.properties = properties;
    }

  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    rename( attributes, "assertionLevel", "level" );

    return new FlowHolder( properties, (String) type, (String) value );
    }

  public static class FlowHolder extends BaseHolder
    {
    private final Properties properties;
    String name;
    boolean skipIfSinkExists = false;
    AssemblyFactory.Assembly assembly = new AssemblyFactory.Assembly();
    TapMap map = new TapMap();
    AssertionLevel assertionLevel;

    Flow flow;

    public FlowHolder( Properties properties, String type, String name )
      {
      super( type );
      this.properties = properties;
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

    public void setAssertionLevel( AssertionLevel assertionLevel )
      {
      this.assertionLevel = assertionLevel;
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

      }

    public Flow connectFlow()
      {
      if( flow != null )
        return flow;

      FlowConnector flowConnector = new FlowConnector( properties );

      if( assertionLevel != null )
        flowConnector.setAssertionLevel( assertionLevel );

      if( map.getTraps().size() == 0 )
        {
        if( map.getSources().size() == 1 && map.getSinks().size() == 1 && assembly.getTails().size() == 1 )
          flow = flowConnector.connect( name, map.getSource(), map.getSink(), assembly.getTail() );
        else if( map.getSources().size() == 1 )
          flow = flowConnector.connect( name, map.getSource(), map.getSinks(), assembly.getTailsArray() );
        else if( map.getSinks().size() == 1 )
          flow = flowConnector.connect( name, map.getSources(), map.getSink(), assembly.getTail() );
        else
          flow = flowConnector.connect( name, map.getSources(), map.getSinks(), assembly.getTailsArray() );
        }
      else
        if( map.getSources().size() == 1 && map.getSinks().size() == 1 && map.getTraps().size() == 1 && assembly.getTails().size() == 1 )
          {
          flow = flowConnector.connect( name, map.getSource(), map.getSink(), map.getTrap(), assembly.getTail() );
          }
        else
          {
          flow = flowConnector.connect( name, map.getSources(), map.getSinks(), map.getTraps(), assembly.getTailsArray() );
          }

      flow.setSkipIfSinkExists( skipIfSinkExists );

      return flow;
      }
    }

  }