import cascading.cascade.Cascade
import cascading.groovy.Cascading

def cascading = new Cascading()
def builder = cascading.builder();

Cascade fetchSort = builder.cascade("fetchSort")
  {
    flow("fetch")
      {
        source('http://www.census.gov/genealogy/names/dist.all.last', scheme: text())

        copy()

        sink('output/imported', scheme: text(), delete: true)
      }

    flow("filter")
      {
        source('output/imported', scheme: text(["lines"]))

        cut(/\s+/, results: [1])
        group()

        sink('output/sorted', scheme: text(), delete: true)
      }
  }

fetchSort.complete();