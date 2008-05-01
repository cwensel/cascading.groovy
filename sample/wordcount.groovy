import cascading.flow.Flow
import cascading.groovy.Cascading

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

// This is a query and the 'content-length' is not returned, so it must be prefetched.
File dir = new File('output/fetched')
File data = new File(dir, "fetch.txt")

if( !data.exists() )
{
  dir.mkdirs()

  URL url = new URL('http://www.i-r-genius.com/cgi-bin/lipsum.cgi?qty=400&unit=k&pl=r&ps=6&pp=n&pt=1&format=t&li=1')

  url.openStream().readLines().each() {line ->
    data.append line
    data.append '\n'
  }
}

def cascading = new Cascading()
def builder = cascading.builder();

Flow flow = builder.flow("wordcount")
  {
    source(data, scheme: text())

    tokenize(/[.,]*\s+/) // output new tuple for each split
    group() // group on first field, by default
    count() // creates 'count' field, by default
    group(["count"])

    sink('output/counted', delete: true)
  }

//cascading.setDebugLogging()

try
{
  flow.complete() // execute the flow
}
catch (Exception exception)
{
  exception.printStackTrace()
};