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

package cascading.groovy.factory.regex;

import java.util.Map;

import cascading.operation.Operation;
import cascading.operation.regex.RegexFilter;
import cascading.tuple.Fields;

/**
 *
 */
public class RegexFilterFactory extends RegexOperationFactory
  {
  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    String pattern = getPattern( value, attributes );
    Boolean removeMatch = (Boolean) attributes.remove( "removeMatch" );

    return (Operation) makeInstance( RegexFilter.class, null, pattern, removeMatch );
    }
  }