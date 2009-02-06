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

import cascading.groovy.factory.OperationFactory;

/**
 *
 */
public class RegexOperationFactory extends OperationFactory
  {
  protected String getPattern( Object value, Map attributes )
    {
    String pattern = (String) attributes.remove( "pattern" );

    if( pattern == null )
      pattern = (String) value;

    if( pattern == null )
      throw new RuntimeException( "pattern value is required" );

    return pattern;
    }
  }
