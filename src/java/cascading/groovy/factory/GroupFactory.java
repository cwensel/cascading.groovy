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