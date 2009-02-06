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

package cascading.groovy.commands

import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Shell

class EchoCommand
extends CommandSupport
{

  EchoCommand(final Shell shell, final String name, final String alias)
  {
    super(shell, name, alias)
  }

  EchoCommand(final Shell shell)
  {
    super(shell, 'echo', '\\ec')
  }

  Object execute(final List args)
  {
    args.each() {arg ->
      io.out.println(arg.getClass().getName())
    }

    io.out.println(args.join(' ')) //  TODO: i18n
  }
}
