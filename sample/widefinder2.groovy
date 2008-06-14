import cascading.cascade.Cascade
import cascading.flow.FlowException
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

def dataUrl = 'http://files.cascading.org/apache.200.txt.gz'

String logs = 'output/logs/'
String output = 'output/results'

def APACHE_COMMON_REGEX = /^([^ ]*) +[^ ]* +[^ ]* +\[([^]]*)\] +\"([^ ]*) ([^ ]*) [^ ]*\" ([^ ]*) ([^ ]*) \"([^ ]*)\".*$/
def APACHE_COMMON_GROUPS = [1, 2, 3, 4, 5, 6, 7]
def APACHE_COMMON_FIELDS = ["ip", "time", "method", "url", "status", "size", "referrer"]

//def URL_PATTERN = /\/ongoing\/When\/\d\d\dx\/\d\d\d\d\/\d\d\/\d\d\/[^ .]+/
def URL_PATTERN = /^\/archives\/.*$/

def EXCEPT_URL_PATTERN = /(-)|(^http:\/\/(www.)?example.org.*)/

def cascading = new Cascading()
def builder = cascading.builder();

Cascade cascade;

try
{
  cascade = builder("widefinder2")
    {

      flow("fetcher", skipIfSinkExists: true) // no unnecessary polling
        {
          source(dataUrl) // gz is assumed text scheme
          copy()
          sink(logs) // inherits scheme from source
        }

      flow("counter")
        {
          map
          {
            source(name: "process", path: logs, scheme: text())

            sink(name: "articles", path: output + "/articles", scheme: sequence(["url", "count"]), delete: true)
            sink(name: "bytes", path: output + "/bytes", scheme: sequence(["url", "bytes"]), delete: true)
            sink(name: "ip", path: output + "/ip", scheme: sequence(["ip", "count"]), delete: true)
            sink(name: "referrer", path: output + "/referrer", scheme: sequence(["referrer", "count"]), delete: true)
            sink(name: "404", path: output + "/404", scheme: sequence(["url", "count"]), delete: true)
          }

          assembly("counter")
            {
              // parse apache log, given regex groups are matched with respective field names
              regexParser(pattern: APACHE_COMMON_REGEX, groups: APACHE_COMMON_GROUPS, declared: APACHE_COMMON_FIELDS)

              // only consider GET requests
              filter(args: ["method"], pattern: "GET")

              branch("success")
                {
                  filter(args: ["status"], pattern: "(200)|(304)")

                  branch("bytes")
                    {
                      // force - to be zero for summing
                      replaceAll(args: ["size"], decl: ["clean_size"], pattern: "-", replacement: "0", res: ["url", "clean_size"])

                      group(groupBy: ["url"])

                      sum(args: ["clean_size"], decl: ["bytes"])
                    }

                  branch("valid_articles")
                    {
                      // keep articles
                      filter(args: ["url"], pattern: URL_PATTERN)

                      branch("articles")
                        {
                          group(groupBy: ["url"])

                          count(args: ["url"])
                        }

                      branch("ip")
                        {
                          group(groupBy: ["ip"])

                          count(args: ["ip"])
                        }

                      branch("referrer")
                        {
                          // ignore self referrers
                          filter(args: ["referrer"], pattern: EXCEPT_URL_PATTERN, removeMatch: true)

                          group(groupBy: ["referrer"])

                          count(args: ["referrer"])
                        }
                    }
                }

              branch("404")
                {
                  filter(args: ["status"], pattern: "404")

                  group(groupBy: ["url"])

                  count(args: ["url"])
                }

            }

        }

      flow("reporter")
        {
          map
          {
            source(name: "articles", path: output + "/articles", scheme: sequence(["url", "count"]), delete: true)
            source(name: "bytes", path: output + "/bytes", scheme: sequence(["url", "bytes"]), delete: true)
            source(name: "ip", path: output + "/ip", scheme: sequence(["ip", "count"]), delete: true)
            source(name: "referrer", path: output + "/referrer", scheme: sequence(["referrer", "count"]), delete: true)
            source(name: "404", path: output + "/404", scheme: sequence(["url", "count"]), delete: true)

            sink(name: "articles", path: output + "/top_articles", scheme: text(), delete: true)
            sink(name: "bytes", path: output + "/top_bytes", scheme: text(), delete: true)
            sink(name: "ip", path: output + "/top_ip", scheme: text(), delete: true)
            sink(name: "referrer", path: output + "/top_referrer", scheme: text(), delete: true)
            sink(name: "404", path: output + "/top_404", scheme: text(), delete: true)
          }

          assembly("reporter")
            {
              assembly("articles")
                {
                  sort(["count"], reverse: true)
                }

              assembly("bytes")
                {
                  sort(["bytes"], reverse: true)
                }

              assembly("ip")
                {
                  sort(["count"], reverse: true)
                }

              assembly("referrer")
                {
                  sort(["count"], reverse: true)
                }

              assembly("404")
                {
                  sort(["count"], reverse: true)
                }
            }
        }
    }

}
catch (FlowException exception)
{
  print exception.getMessage();
  exception.writeDOT("widefinder2.dot") // write graph to disk for inspection
  return;
}

cascading.enableInfoLogging()

try
{
  cascade.complete() // execute the flow
}
catch (Exception exception)
{
  exception.printStackTrace()
}
