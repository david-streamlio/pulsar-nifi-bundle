# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

### Primary Build
```bash
export JAVA_HOME=`/usr/libexec/java_home -v 21`
mvn clean package
```
This builds the NAR files and generates a Docker image with tag `streamnative/nifi`.

### Testing
```bash
mvn test
```
Run unit tests across all modules.

### Single Module Development
Navigate to specific modules for focused development:
- `nifi-pulsar-client-service-api` - Client service API definitions
- `nifi-pulsar-client-service` - Client service implementation  
- `nifi-pulsar-processors` - Core Pulsar processors
- `nifi-pulsar-nar` - NAR packaging for processors
- `nifi-pulsar-client-service-nar` - NAR packaging for client service

## Architecture Overview

This is a Maven multi-module project that provides Apache NiFi processors for Apache Pulsar integration. The codebase follows NiFi's extension patterns with clear separation between APIs, implementations, and packaging.

### Module Structure
- **nifi-pulsar-client-service-api**: Defines service interfaces and authentication APIs for Pulsar clients
- **nifi-pulsar-client-service**: Implements the client service with connection management, validation, and JWT authentication
- **nifi-pulsar-processors**: Contains the core processors (`PublishPulsar`, `ConsumePulsar`, `PublishPulsarRecord`, `ConsumePulsarRecord`) and supporting utilities
- **nifi-pulsar-nar**: NAR packaging for the processors
- **nifi-pulsar-client-service-nar**: NAR packaging for the client service
- **docker-image**: Docker image configuration for testing

### Key Components
- **Abstract Base Classes**: `AbstractPulsarConsumerProcessor` and `AbstractPulsarProducerProcessor` provide common functionality
- **Client Service**: Centralized Pulsar client management with LRU caching
- **Authentication**: JWT-based authentication service for Pulsar clusters
- **Utilities**: Publisher pooling, property mapping, and validation helpers

### Dependencies
- NiFi 2.1.0
- Apache Pulsar 3.3.7  
- Java 21
- Maven build system with specialized NiFi NAR plugin

## Docker Testing

Start test container:
```bash
docker run --name nifi -d -p 8443:8443 \
  -e SINGLE_USER_CREDENTIALS_USERNAME=admin \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB \
  streamnative/nifi
```

Access at: https://localhost:8443/nifi/login

Enable JVM debugging:
```bash
docker run -d --name nifi \
  -p 8443:8443 -p 8000:8000 \
  -e NIFI_JVM_DEBUGGER=true \
  -e SINGLE_USER_CREDENTIALS_USERNAME=admin \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB \
  streamnative/nifi
```

## Development Notes

- Always ensure Java 21 is configured before building
- NAR files are the deployment artifacts for NiFi processors
- Client service uses LRU caching for connection efficiency
- All processors extend NiFi's abstract base classes
- Docker image includes built NAR files for immediate testing