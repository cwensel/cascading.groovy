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
