import cascading.flow.Flow
import cascading.groovy.Cascading

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
    source(data)

    tokenize(/[.,]*\s+/) // output new tuple for each split
    group() // group on first field, by default
    count() // creates 'count' field, by default
    group(["count"])

    sink('output/counted', delete: true)
  }

flow.complete();