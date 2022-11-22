# DoiT dataflow sample code

This is a repository with code samples for different dataflow use-cases.

## Description

Because we are working with a lot of clients with alot of different use-cases we created this repository.
We are trying to keep the code so generic as possible so different client can use them as blueprint.

* DataGenerator: Is a project that generate data for the examples in the DataPipeline project.
* DataPipeline: Is a Dataflow project that reads files from cloud storage and can store the files to different databases

## Getting Started

### Dependencies

* Java 8
* Maven
* Gcloud

### Installing

* You can install the corresponding package by running the maven install command in the package of your sample code.
```
mvn -pl DataPipeline clean install
```

### Executing program

* Run the program using maven in Dataflow
```
mvn compile exec:java \
  -Dexec.mainClass=DataPipeline \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.args=" \
    --project=${PROJECT_ID} \
    --input=${BUCKET} \
    --bigqueryDatasetId= ${bigqueryDatasetId} \
    --bigqueryTableId= ${bigqueryTableId}\
    --bigqueryProjectId= ${bigqueryProjectId} \
    --region=${REGION} \
    --runner=DataflowRunner" 
```

## Version History

* 0.1
    * Initial Release

## License

This project is licensed under the MIT License - see the LICENSE.md file for details
