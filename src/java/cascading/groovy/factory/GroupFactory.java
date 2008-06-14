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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class GroupFactory extends BaseFactory
  {
  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    rename( attributes, "groupBy", "by" );
    rename( attributes, "sortBy", "sort" );
    rename( attributes, "declared", "decl" );
    rename( attributes, "reverse", "rev" );

    Set names = new HashSet();
    Collections.addAll( names, "groupBy", "sortBy", "declared", "reverse" );

    Map joinFields = new HashMap( attributes );

    for( Object o : names )
      joinFields.remove( o );

    for( Object o : joinFields.keySet() )
      attributes.remove( o );

    if( value != null )
      return new GroupHolder( (String) type, joinFields, (ArrayList) value );

    return new GroupHolder( (String) type, joinFields );
    }

  /**
   *
   */
  public static class GroupHolder extends PipeHolder
    {
    Comparable[] groupBy;
    Comparable[] sortBy;
    Comparable[] declared;
    Boolean reverse = false;
    Map joinFields;

    public GroupHolder( String type, Map joinFields )
      {
      super( type );
      this.joinFields = joinFields;
      }

    public GroupHolder( String name, Map joinFields, ArrayList value )
      {
      super( name );
      this.joinFields = joinFields;
      groupBy = (Comparable[]) value.toArray( new Comparable[value.size()] );
      }

    public Object createInstance( Object parent )
      {
      if( parent instanceof FlowFactory.FlowHolder )
        parent = ( (FlowFactory.FlowHolder) parent ).assembly;

      AssemblyFactory.Assembly assembly = (AssemblyFactory.Assembly) parent;

      if( getType().equalsIgnoreCase( "group" ) || getType().equalsIgnoreCase( "sort" ) )
        return makeGroupBy( assembly );

      if( getType().equalsIgnoreCase( "join" ) )
        return makeCoGroup( assembly );

      throw new RuntimeException( "type: " + getType() + " was not found" );
      }

    private Pipe makeCoGroup( AssemblyFactory.Assembly assembly )
      {
      List params = new ArrayList();

      params.add( assembly.getType() );

      for( Object joinName : joinFields.keySet() )
        {
        Pipe pipe = (Pipe) assembly.removeTail( (String) joinName );

        if( pipe == null )
          throw new RuntimeException( "unable to find pipe with type: " + joinName );

        List fieldsList = (List) joinFields.get( joinName );
        Fields fields = new Fields( (Comparable[]) fieldsList.toArray( new Comparable[fieldsList.size()] ) );

        params.add( pipe );
        params.add( fields );
        }

      return makePipe( CoGroup.class, params.toArray() );
      }

    private Pipe makeGroupBy( AssemblyFactory.Assembly assembly )
      {
      String name = assembly.getName();
      Pipe pipe = null;

      if( assembly.getLastChild() != null )
        pipe = (Pipe) assembly.getLastChild();
      else
        pipe = new Pipe( name, assembly.getPreviousPipe() ); // type the split

      Fields groupFields = null;
      Fields sortFields = null;

      if( groupBy != null )
        groupFields = new Fields( groupBy );

      if( sortBy != null )
        sortFields = new Fields( sortBy );

      if( reverse != null && !reverse ) // set null, so we don't send it to the ctor
        reverse = null;

      if( sortFields == null && reverse != null ) // if not null, then is true
        sortFields = groupFields;

      return makePipe( GroupBy.class, name, pipe, groupFields, sortFields, reverse );
      }

    public String toString()
      {
      return "GroupHolder{" + "groupBy=" + ( groupBy == null ? null : Arrays.asList( groupBy ) ) + ", sortBy=" + ( sortBy == null ? null : Arrays.asList( sortBy ) ) + ", reverse=" + reverse + '}';
      }
    }
  }