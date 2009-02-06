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
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.MultiTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import groovy.util.FactoryBuilderSupport;

/**
 *
 */
public class TapFactory extends BaseFactory
  {
  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    if( type != null && type.equals( "tap" ) )
      return value;

    return new TapHolder( (String) type, value );
    }

  public static class TapHolder extends BaseHolder
    {
    List<String> paths = new ArrayList<String>();
    Scheme scheme;
    Comparable[] fields;
    boolean delete = false;
    private Tap tap;

    public TapHolder( String type )
      {
      super( type );
      }

    public TapHolder( String type, Object path )
      {
      super( type );
      setPath( path );
      }

    public TapHolder( String type, Object path, Scheme scheme, Comparable[] fields, boolean delete )
      {
      super( type );
      setPath( path );
      this.scheme = scheme;
      this.fields = fields;
      this.delete = delete;
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

    public void setScheme( Scheme scheme )
      {
      this.scheme = scheme;
      }

    public void setChild( Object child )
      {
      setScheme( (Scheme) child );
      }

    public void handleParent( Object parent )
      {
      tap = createTap();

      if( parent instanceof EndPointFactory.EndPointHolder )
        ( (EndPointFactory.EndPointHolder) parent ).setTap( tap );
      }

    public Tap createTap()
      {
      if( tap != null )
        return tap;

      Tap[] taps = new Tap[paths.size()];

      for( int i = 0; i < paths.size(); i++ )
        taps[ i ] = createTap( paths.get( i ) );

      if( taps.length == 1 )
        return taps[ 0 ];

      return new MultiTap( taps );
      }

    private Tap createTap( String path )
      {
      if( type.equalsIgnoreCase( "hfs" ) )
        return createHfs( path );
      else if( type.equals( "lfs" ) )
        return createLfs( path );
      else
        throw new RuntimeException( "unkown tap type: " + type );
      }

    private Lfs createLfs( String path )
      {
      if( scheme == null )
        return new Lfs( new TextLine(), path, delete );
      else
        return new Lfs( scheme, path, delete );
      }

    private Hfs createHfs( String path )
      {
      if( scheme == null && path.matches( ".*[.](txt|gz)[^/]?$" ) )
        scheme = SchemeFactory.createDefaultTextLine();

      if( scheme == null && fields == null )
        throw new RuntimeException( "must provide scheme or fields in tap with path: " + path );

      if( scheme == null )
        return new Hfs( new Fields( fields ), path, delete );
      else
        return new Hfs( scheme, path, delete );
      }
    }

  }