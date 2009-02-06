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
import java.util.List;
import java.util.Map;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.operation.AssertionLevel;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class CascadeFactory extends BaseFactory
  {
  String name;

  public CascadeFactory()
    {
    }

  public CascadeFactory( String name )
    {
    this.name = name;
    }

  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    if( value == null )
      value = name;

    rename( attributes, "assertionLevel", "level" );

    return new CascadeHolder( (String) type, (String) value );
    }

  public static class CascadeHolder extends BaseHolder
    {
    String name;
    List<Flow> flows = new ArrayList<Flow>();
    List<FlowFactory.FlowHolder> flowHolders = new ArrayList<FlowFactory.FlowHolder>();
    AssertionLevel assertionLevel;

    public CascadeHolder( String type, String name )
      {
      super( type );
      this.name = name;
      }

    public String getName()
      {
      return name;
      }

    public void setChild( Object child )
      {
      if( child instanceof FlowFactory.FlowHolder )
        {
        FlowFactory.FlowHolder flowHolder = (FlowFactory.FlowHolder) child;

        flowHolder.setAssertionLevel( assertionLevel );
        flowHolders.add( flowHolder );
        }
      }

    public void handleParent( Object parent )
      {

      }

    private void renderFlows()
      {
      for( FlowFactory.FlowHolder flowHolder : flowHolders )
        flows.add( flowHolder.connectFlow() );
      }

    public Cascade connectCascade()
      {
      renderFlows();

      return new CascadeConnector().connect( name, (Flow[]) flows.toArray( new Flow[0] ) );
      }
    }

  }