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

import java.util.HashMap;
import java.util.Map;

import cascading.tap.Tap;

/**
 *
 */
public class TapMap
  {
  public static final String DEFAULT_NAME = "_DEFAULT_";

  String name;
  Map<String, Tap> sources = new HashMap<String, Tap>();
  Map<String, Tap> sinks = new HashMap<String, Tap>();
  Map<String, Tap> traps = new HashMap<String, Tap>();

  public TapMap()
    {
    }

  public String getName()
    {
    return name;
    }

  public void setName( String name )
    {
    this.name = name;
    }

  public Map<String, Tap> getSources()
    {
    return sources;
    }

  public void setSources( Map<String, Tap> sources )
    {
    this.sources = sources;
    }

  public void addSource( String name, Tap tap )
    {
    if( sources.containsKey( name ) )
      throw new RuntimeException( "cannot accept duplicate tap names: " + name );

    sources.put( name, tap );
    }

  public Tap getSource( String name )
    {
    return sources.get( name );
    }

  public Tap getSource()
    {
    return sources.values().iterator().next();
    }

  public Map<String, Tap> getSinks()
    {
    return sinks;
    }

  public void setSinks( Map<String, Tap> sinks )
    {
    this.sinks = sinks;
    }

  public void addSink( String name, Tap tap )
    {
    if( sinks.containsKey( name ) )
      throw new RuntimeException( "cannot accept duplicate tap names: " + name );

    sinks.put( name, tap );
    }

  public Tap getSink( String name )
    {
    return sinks.get( name );
    }

  public Tap getSink()
    {
    return sinks.values().iterator().next();
    }

  public Map<String, Tap> getTraps()
    {
    return traps;
    }

  public void setTraps( Map<String, Tap> traps )
    {
    this.traps = traps;
    }

  public void addTrap( String name, Tap tap )
    {
    if( traps.containsKey( name ) )
      throw new RuntimeException( "cannot accept duplicate tap names: " + name );

    traps.put( name, tap );
    }

  public Tap getTrap( String name )
    {
    return traps.get( name );
    }

  public Tap getTrap()
    {
    return traps.values().iterator().next();
    }

  public String toString()
    {
    if( traps.isEmpty() )
      return "TapMap{" + "name='" + name + '\'' + ", sources=" + sources + ", sinks=" + sinks + '}';
    else
      return "TapMap{" + "name='" + name + '\'' + ", sources=" + sources + ", sinks=" + sinks + ", traps=" + traps + '}';
    }
  }
