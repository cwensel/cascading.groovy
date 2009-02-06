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
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowSkipIfSinkExists;
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

      FlowConnector.setAssertionLevel( properties, assertionLevel );

      FlowConnector flowConnector = new FlowConnector( properties );

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
      else if( map.getSources().size() == 1 && map.getSinks().size() == 1 && map.getTraps().size() == 1 && assembly.getTails().size() == 1 )
        {
        flow = flowConnector.connect( name, map.getSource(), map.getSink(), map.getTrap(), assembly.getTail() );
        }
      else
        {
        flow = flowConnector.connect( name, map.getSources(), map.getSinks(), map.getTraps(), assembly.getTailsArray() );
        }

      if( skipIfSinkExists )
        flow.setFlowSkipStrategy( new FlowSkipIfSinkExists() );

      return flow;
      }
    }

  }