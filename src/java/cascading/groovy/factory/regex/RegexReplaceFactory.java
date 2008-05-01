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

package cascading.groovy.factory.regex;

import java.util.Map;

import cascading.operation.Operation;
import cascading.operation.regex.RegexReplace;
import cascading.tuple.Fields;

/**
 *
 */
public class RegexReplaceFactory extends RegexOperationFactory
  {
  Boolean replaceAll = null;

  public RegexReplaceFactory()
    {
    }

  public RegexReplaceFactory( Boolean replaceAll )
    {
    this.replaceAll = replaceAll;
    }

  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    String pattern = getPattern( value, attributes );
    String replacement = (String) attributes.remove( "replacement" );

    if( replacement == null )
      throw new RuntimeException( "replacement value is required" );

    // let argument override
    Boolean replaceAllArg = (Boolean) attributes.remove( "replaceAll" );

    if( replaceAllArg != null )
      replaceAll = replaceAllArg;

    return (Operation) makeInstance( RegexReplace.class, null, pattern, replacement, replaceAll );
    }

  }