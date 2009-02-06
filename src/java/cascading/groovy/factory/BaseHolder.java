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

/**
 *
 */
public abstract class BaseHolder
  {
  String type;

  protected BaseHolder()
    {
    }

  protected BaseHolder( String type )
    {
    this.type = type;
    }

  public String getType()
    {
    return type;
    }

  public void setType( String type )
    {
    this.type = type;
    }

  public abstract void setChild( Object child );

  public abstract void handleParent( Object parent );
  }
