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

package cascading.groovy.factory.assertion;

import java.util.Map;

import cascading.groovy.factory.regex.RegexOperationFactory;
import cascading.operation.Operation;
import cascading.tuple.Fields;

/**
 *
 */
public class AssertMatchesFactory extends RegexOperationFactory
  {
  private Class type;

  public AssertMatchesFactory( Class type )
    {
    this.type = type;
    }

  @Override
  protected Operation makeOperation( Object value, Map attributes, Fields declaredFields )
    {
    String pattern = getPattern( value, attributes );
    Boolean negateMatch = (Boolean) attributes.remove( "negateMatch" );

    return (Operation) makeInstance( type, null, pattern, negateMatch );
    }
  }