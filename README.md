<p align="center">
  <img src="https://github.com/bigconnect/bigconnect/raw/master/docs/logo.png" alt="BigConnect Logo"/>
  <br>
  The multi-model Big Graph Store<br>
</p>

# BigConnect Data Processing Plugins

Data Processing plugins provide the capability to react when something changes in BigConnect, such as:

* Vertices & Edges were added or deleted
* A property was added, updated or deleted

Data processing works by using queues. You need to manually push an object or property to the DataWorker queue to be
picked up by the DataWorkerRunner process.

Have a look at the [Architecture and Concepts](https://docs.bigconnect.io/cloud/bigconnect-core/architecture-and-concepts) to
better understand how the mechanism works.

[BigConnect Explorer](https://docs.bigconnect.io/cloud/bigconnect-explorer) uses these plugins to apply enrichments to various objects imported in the system.

This repository contains plugins for various data enrichment capabilities:

* Extraction of text from documents
* Language detection  
* Named Entity Recognition
* Sentiment Analysis  
* Image metadata extraction
* Ontology mapping based on mime type
* URL Facebook engagement
* Audio metadata extraction
* Video metadata extraction
* Creation of video previews
* Video frame extraction
* Video audio track extraction
* Execution of custom Groovy scripts  
* Google Translate
* Google Speech-to-Text
* Detection of mime types for incoming data  
* etc...

## Installation
Build and install the plugins using Maven:

```
mvn clean install
```

## Installing plugins
In order to install a Data Processing plugin just copy the jar file of the plugin to the ```lib/``` folder of
[BigConnect Core](https://github.com/bigconnect/bigconnect) or BigConnect Explorer

## Contributing
Contributions are warmly welcomed and greatly appreciated. Here are a few ways you can contribute:

* Start by fixing some [issues](https://github.com/bigconnect/dataworker-plugins/issues?q=is%3Aissue+is%3Aopen)
* Submit a Pull Request with your fix

## Getting help & Contact
* [Official Forum](https://community.bigconnect.io/)
* [LinkedIn Page](https://www.linkedin.com/company/bigconnectcloud/)

## License
BigConnect is an open source product licensed under AGPLv3.
