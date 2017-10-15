A simple test implementation of the Composed Message Processor using apache camel.

image::http://www.enterpriseintegrationpatterns.com/img/DistributionAggregate.gif[Composed Message Processor]

The implementation takes a csv list of superheroes* and groups the characters by gender.
Each group is exported to a separate CSV file (with header).


*list was taken from: https://github.com/fivethirtyeight/data/tree/master/comic-characters
