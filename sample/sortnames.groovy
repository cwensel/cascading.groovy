import cascading.cascade.Cascade
import cascading.groovy.Cascading

/*
* Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

def cascading = new Cascading()
def builder = cascading.builder();

Cascade fetchSort = builder.cascade("fetchSort")
  {
    flow("fetch", skipIfSinkExists: true)
      {
        source('http://www.census.gov/genealogy/names/dist.all.last', scheme: text())

        copy()

        sink('output/imported', scheme: text())
      }

    flow("filter")
      {
        source('output/imported', scheme: text())

        cut(/\s+/, results: [1])
        group()

        sink('output/sorted', scheme: text(), delete: true)
      }
  }

cascading.enableInfoLogging()

try
{
  fetchSort.complete()
}
catch (Exception exception)
{
  exception.printStackTrace()
}
