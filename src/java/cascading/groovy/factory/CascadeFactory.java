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