# Storm Tutorial

Demonstrates realtime stream processing with the [Storm](https://github.com/nathanmarz/storm) framework.

This tutorial shows a simple financial application:

* The data source ("spout" in Storm parlance) is a price feed with live price data for a set of financial instruments.
* The data is consumed by candlestick chart generators sampling it an collecting min/max and open/close data.

The tutorial demonstrates:

* Spouts, the data sources.
* Bolts, the functions to consume and transform data.
* Topology, setting up the data stream flow.
* How to add custom serialization formats for new data types.


## License

Copyright (C) 2011 Martin Jul (www.mjul.com)

Distributed under the MIT License. See the LICENSE file for details.


## About the Author

Martin Jul is a software architect and partner in Ative, a
Copenhagen-based consultancy specialized in doing and teaching lean
software development.

His work is currently focused on building distributed,
high-performance low-latency financial trading applications.

He is also organizing of the Copenhagen Clojure meet-ups:

* Twitter: [@mjul](http://twitter.com/mjul)
* Work: [Ative](http://www.ative.dk) 
* Blog: [Ative at Work](http://community.ative.dk/blogs/)
* Copenhagen Clojure Meet-Up [dates here](http://www.ative.dk/om-ative/arrangementer.aspx)
