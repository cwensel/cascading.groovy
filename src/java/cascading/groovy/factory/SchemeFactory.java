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

import java.util.List;
import java.util.Map;

import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tuple.Fields;
import groovy.util.FactoryBuilderSupport;


/**
 *
 */
public class SchemeFactory extends BaseFactory
  {
  public Object newInstance( FactoryBuilderSupport builder, Object type, Object value, Map attributes ) throws InstantiationException, IllegalAccessException
    {
    if( type.equals( "text" ) )
      return createTextLine( value, attributes );
    else if( type.equals( "sequence" ) )
      return createSequenceFile( value, attributes );

    throw new RuntimeException( "unknown scheme type: " + type );
    }

  protected Scheme createTextLine( Object value, Map attributes )
    {
    Fields fields = createFields( (List) value );

    if( fields == null )
      fields = createFields( (List) findRemove( attributes, "fields" ) );

    if( fields != null )
      return new TextLine( fields );
    else
      return createDefaultTextLine();
    }

  static TextLine createDefaultTextLine()
    {
    return new TextLine( new Fields( "line" ) );
    }

  protected Scheme createSequenceFile( Object value, Map attributes )
    {
    Fields fields = createFields( (List) value );

    if( fields == null )
      fields = createFields( (List) findRemove( attributes, "fields" ) );

    if( fields != null )
      return new SequenceFile( fields );
    else
      throw new RuntimeException( "fields are required" );
    }
  }
