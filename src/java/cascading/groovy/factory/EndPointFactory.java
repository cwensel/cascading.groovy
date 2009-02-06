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

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class EndPointFactory extends BaseFactory
  {
  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    Scheme sourceScheme = null;

    // we inherit the source scheme by default
    if( ( type.equals( "sink" ) || type.equals( "trap" ) ) && builder.getCurrent() instanceof FlowFactory.FlowHolder ) // find source
      {
      TapMap tapMap = ( (FlowFactory.FlowHolder) builder.getCurrent() ).map;

      if( tapMap.sources.size() == 1 )
        sourceScheme = tapMap.getSource().getScheme();
      }

    retype( attributes );

    return new EndPointHolder( (String) type, value, sourceScheme );
    }

  void retype( Map attributes )
    {
    Object tap = attributes.get( "tap" );

    if( tap instanceof TapFactory.TapHolder )
      attributes.put( "tap", ( (TapFactory.TapHolder) tap ).createTap() );
    }

  public class EndPointHolder extends BaseHolder
    {
    Object argValue;
    String name = TapMap.DEFAULT_NAME;
    List<String> paths = new ArrayList<String>();
    Comparable[] fields;
    Scheme sourceScheme;
    Scheme scheme;
    boolean delete = false;
    Tap tap;

    public EndPointHolder( String type, Object argValue, Scheme sourceScheme )
      {
      super( type );
      this.argValue = argValue;
      this.sourceScheme = sourceScheme;
      }

    public void setPath( Object path )
      {
      if( path == null )
        return;

      if( path instanceof List )
        {
        List values = (List) path;

        for( Object value : values )
          paths.add( value.toString() );
        }
      else
        paths.add( path.toString() );
      }


    public void setTap( Tap tap )
      {
      this.tap = tap;
      }

    public void setChild( Object child )
      {
      if( child instanceof Tap )
        setTap( (Tap) child );
      }

    public void handleParent( Object parent )
      {
      // add name/tap pair to parent
      if( !( parent instanceof TapMap ) )
        return;

      if( scheme == null && fields == null )
        scheme = sourceScheme;

      if( tap == null )
        {
        if( paths.size() == 0 )
          {
          setPath( argValue );
          argValue = null;
          }

        new TapFactory.TapHolder( "hfs", paths, scheme, fields, delete ).handleParent( this );
        }

      if( argValue != null )
        name = (String) argValue;

      if( tap == null )
        throw new RuntimeException( "no tap specified in " + getType() + "endpoint named " + name );

      TapMap tapMap = (TapMap) parent;

      if( getType().equalsIgnoreCase( "source" ) )
        tapMap.addSource( name, tap );
      else if( getType().equalsIgnoreCase( "sink" ) )
        tapMap.addSink( name, tap );
      else if( getType().equalsIgnoreCase( "trap" ) )
        tapMap.addTrap( name, tap );
      else
        throw new RuntimeException( "unknown endpoint type: " + getType() );
      }
    }

  }