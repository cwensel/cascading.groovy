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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.pipe.Pipe;
import groovy.util.AbstractFactory;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class AssemblyFactory extends AbstractFactory
  {
  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    if( builder.getCurrent() instanceof FlowFactory.FlowHolder )
      return new Assembly( value == null ? ( (FlowFactory.FlowHolder) builder.getCurrent() ).getName() : (String) value, null );
    else if( builder.getCurrent() instanceof Assembly )
      return new Assembly( (String) value, ( (Assembly) builder.getCurrent() ).getLastChild() );
    else
      return new Assembly( (String) value );
    }

  public static class Assembly extends BaseHolder
    {
    String name;
    Pipe previousPipe;
    Pipe lastChild;
    Map<String, Pipe> tailMap = new HashMap<String, Pipe>();

    public Assembly()
      {
      }

    public Assembly( String name )
      {
      this.name = name;
      }

    public Assembly( String name, Pipe lastChild )
      {
      this.name = name;
      this.previousPipe = lastChild;
      }

    public String getName()
      {
      return name;
      }

    public void setName( String name )
      {
      this.name = name;
      }

    public void setChild( Object child )
      {

      }

    public void handleParent( Object parent )
      {
      if( parent instanceof AssemblyFactory.Assembly )
        {
        ( (AssemblyFactory.Assembly) parent ).addTails( getTailMap() );
        ( (AssemblyFactory.Assembly) parent ).removeTail( ( (AssemblyFactory.Assembly) parent ).getLastChild() );
        }
      }

    public Pipe getPreviousPipe()
      {
      return previousPipe;
      }

    public Pipe getLastChild()
      {
      return lastChild;
      }

    public void setLastChild( Pipe lastChild )
      {
      this.lastChild = lastChild;
      }


    public String toString()
      {
      return "Assembly{" + "name='" + name + '\'' + ", previousPipe=" + previousPipe + ", lastChild=" + lastChild + ", tailMap=" + tailMap + '}';
      }

    public Map getTailMap()
      {
      return tailMap;
      }

    public void addTail( String name, Pipe node )
      {
      tailMap.put( name, node );
      }

    public void addTail( Pipe node )
      {
      addTail( node.getName(), node );
      }

    public void addTails( Map tails )
      {
      tailMap.putAll( tails );
      }

    public Pipe getTail( String name )
      {
      return (Pipe) tailMap.get( name );
      }

    public Pipe getTail()
      {
      return tailMap.values().iterator().next();
      }

    public Pipe removeTail( String name )
      {
      return (Pipe) tailMap.remove( name );
      }

    public void removeTail( Pipe tail )
      {
      if( tail != null )
        tailMap.remove( tail.getName() );
      }

    public Collection<Pipe> getTails()
      {
      return tailMap.values();
      }

    public Pipe[] getTailsArray()
      {
      return tailMap.values().toArray( new Pipe[tailMap.size()] );
      }

    public Collection getHeads()
      {
      Set<Pipe> heads = new HashSet<Pipe>();

      for( Pipe pipe : tailMap.values() )
        Collections.addAll( heads, pipe.getHeads() );

      return heads;
      }

    }
  }