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

  public String toString()
    {
    return "TapMap{" + "name='" + name + '\'' + ", sources=" + sources + ", sinks=" + sinks + '}';
    }
  }
