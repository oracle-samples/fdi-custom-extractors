# Custom Extractor SDK

[**Oracle Fusion Data Intelligence Custom Extractor SDK**]() enables developers to create, test, and deploy custom extractors for Oracle Fusion Data Intelligence (FDI). The SDK provides an extensible framework and ready-to-use scaffolding for building extractors with custom business logic, helping organizations integrate diverse data sources efficiently.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Building](#building)
- [Deployment](#deployment)
- [Testing](#testing)
- [Samples](#samples)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)
- [Changelog](#changelog)

---

## Features

**Custom Extractor SDK** simplifies the development of data extractors that connect, query, and extract data from diverse sources into Oracle Fusion Data Intelligence.  
Key capabilities include:

- Ready-to-use Maven project scaffolding for rapid setup.
- Support for custom data connection, authentication, and query logic.
- Seamless integration with the Extract Service in FDI.
- CLI tools for deploying and managing custom extractors in Remote Agent environments.
- Sample extractors and templates for common data sources.

---

## Prerequisites

1. **Oracle Fusion Data Intelligence Account**  
   Access to FDI environment and Remote Agent.

2. **Java & Maven**  
   Ensure Java 11+ and Apache Maven are installed and configured.

3. **SDK Package**  
   Obtain the SDK ZIP archive from Oracle distribution.

---

## Installation

1. Unzip the SDK archive:

    ```bash
    unzip fdi-extractor-sdk-24.5.0-SNAPSHOT.zip
    ```

2. Review the SDK contents:

    ```text
    install.sh
    lib/
    ```

3. Install the SDK:

    ```bash
    sh install.sh
    ```

---

## Quick Start

### Generate a New Custom Extractor Project

Use Maven to scaffold a new extractor project:

```bash
mvn archetype:generate    -DarchetypeCatalog=internal    -DarchetypeGroupId=com.oracle.faw.extractservice    -DarchetypeArtifactId=extractservice-archetype    -DarchetypeVersion=24.5.0-SNAPSHOT    -DgroupId=oracle.apps.bi.extractservice.extract.impl    -DartifactId=customextract    -Dversion=1.0-SNAPSHOT
```

### Project Structure

```text
customextract
├── pom.xml
└── src
    ├── main
    │   ├── java
    │   │   └── oracle
    │   │       └── apps
    │   │           └── bi
    │   │               └── extractservice
    │   │                   └── extract
    │   │                       └── impl
    │   │                           ├── CustomApplication.java
    │   │                           └── CustomExtractorImpl.java
    │   └── resources
    └── test
        ├── java
        │   └── oracle
        │       └── apps
        │           └── bi
        │               └── extractservice
        │                   └── extract
        │                       └── impl
        │                           └── ExtractorTest.java
        └── resources
            └── extractor_test.properties
```

---

## Building

Compile the project using Maven:

```bash
mvn clean install
```

This generates a JAR file in the `target` directory:

```
customextract-1.0-SNAPSHOT.jar
```

---

## Deployment

1. Copy the generated JAR to the Remote Agent directory:

    ```bash
    cp target/customextract-1.0-SNAPSHOT.jar /faw/software/custom-extractors/drivers-jar/
    ```

2. Restart the Remote Agent Docker container:

    ```bash
    sudo docker restart <remoteagent-container-name>
    # Example
    sudo docker restart remoteagent
    ```

---

## Testing

Update the `extractor_test.properties` file with your environment settings and run `ExtractorTest` to validate the extractor implementation.

### Test Coverage

- `getSourceType()`  
- `verifyConnection()`  
- `extractDataStores()`  
- `queryData()`  

### Example Reference

Use the provided **COVID Extractor Sample** as a reference implementation for testing and configuration. Modify it to match your data source and logic.

---

## Samples

Sample extractors are available under the [`samples`](./samples) directory. These can be used as templates for building new extractors or extended for different data sources.

Current samples include:

- [`samples/covid-data-extractor`](./samples/covid-data-extractor): sample custom extractor implementation
- [`samples/tcf-data-extractor`](./samples/tcf-data-extractor): sample TCF reader and pipeline implementation for processing FDI Table Change Format data

---

## Contributing

This project welcomes contributions from the community.  
Before submitting a pull request, please review the [contribution guide](./CONTRIBUTING.md).

---

## Security

Please consult the [security guide](./SECURITY.md) for Oracle's responsible vulnerability disclosure process.

---

## License

© 2000–2024 Oracle and/or its affiliates. All rights reserved.

This SDK is licensed under the **Universal Permissive License (UPL) 1.0**.  
See [LICENSE.txt](./LICENSE.txt) for details.  
For more information, visit [Oracle Open Source Licensing](https://oss.oracle.com/licenses/upl/).

---

## Changelog

For detailed version history and updates, please refer to [CHANGELOG.md](./CHANGELOG.md).
