# DAS SQLite
[![License](https://img.shields.io/:license-BSL%201.1-blue.svg)](/licenses/BSL.txt)

[Data Access Service](https://github.com/raw-labs/protocol-das) for SQLite.

## Options

| Name       | Description                           | Default  | Required |
|------------|---------------------------------------|----------|----------|
| `database` | The path to the SQLite database       |          | Yes      |

## How to use

### Prerequisites

You need to have [sbt](https://www.scala-sbt.org/) installed to build the project.

You can install sbt using [sdkman](https://sdkman.io/):
```bash
$ sdk install sbt
```

### Running the server

You can run the server with the following command:
```bash
$ sbt run
```

### Docker

To run the server in a docker container you need to follow these steps:

First, you need to build the project:
```bash
$ sbt "docker:publishLocal"
```

Then you can run the image with the following command:
```bash
$ docker run -p 50051:50051 <image_id>
```
... where `<image_id>` is the id of the image created in the previous step.
This will start the server, typically on port 50051.

You can find the image id by looking at the sbt output or by running:
```bash
$ docker images
```
