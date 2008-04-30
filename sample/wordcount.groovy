import cascading.flow.Flow
import cascading.groovy.Cascading

def cascading = new Cascading()

//cascading.setDebugLogging()

def builder = cascading.builder();

Flow flow = builder.flow("wordcount")
  {
    source('http://www.i-r-genius.com/cgi-bin/lipsum.cgi?qty=400&unit=k&pl=r&ps=6&pp=n&pt=1&format=t&li=1', scheme: text())

    tokenize(/[.,]*\s+/) // output new tuple for each split
    group() // group on first field, by default
    count() // creates 'count' field, by default
    group(["count"])

    sink('output/counted', delete: true)
  }

try
{
  flow.complete() // execute the flow
}
catch (Exception exception)
{
  exception.printStackTrace()
};