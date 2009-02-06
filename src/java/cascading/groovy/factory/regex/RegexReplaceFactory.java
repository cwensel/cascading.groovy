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

    return (Operation) makeInstance( RegexReplace.class, declaredFields, pattern, replacement, replaceAll );
    }

  }