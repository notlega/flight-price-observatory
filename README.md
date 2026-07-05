# Flight Price Observatory

Flight Price Observatory is an automated data engineering and analytics platform designed to continuously collect, store, and analyse historical airfare pricing data. The project focuses on flights departing from Singapore to selected destinations across Asia, with the objective of building a comprehensive historical dataset for trend analysis, exploratory data analysis, and future predictive modelling.

Unlike conventional flight search platforms, which primarily provide current airfare information, this project continuously captures pricing snapshots over time to create a longitudinal dataset. This enables analysis of pricing behaviour across different booking windows, travel seasons, airlines, and routes, providing insights that are not readily available through individual flight searches.

The platform follows a modern data engineering architecture. Automated workflows periodically retrieve flight pricing data through a provider abstraction layer, validate and transform the collected data, and store it within a private cloud-based data lake. The processed datasets are then made available for analytical workloads, dashboards, and machine learning experiments.

The project also serves as a demonstration of modern software and data engineering practices, including automated data collection, cloud-native storage, ETL pipelines, data lake architecture, reproducible analytics, and modular system design. While its primary objective is to generate actionable insights into airfare trends, the system is intentionally designed to remain extensible, allowing additional data providers, destinations, and analytical capabilities to be incorporated in future iterations.

## Table of Contents

- [Objectives](#objectives)
- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running Locally](#running-locally)
- [Documentation](#documentation)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)
- [Notice](#notice)

## Objectives

The primary objective of the Flight Price Observatory is to develop an automated platform that continuously collects, stores, and analyses historical airfare pricing data. By building a long-term dataset of flight prices, the project aims to uncover trends and patterns that are not readily observable through conventional flight search platforms, enabling data-driven insights into airline pricing behaviour.

To achieve this objective, the project has the following goals:

### Build a Historical Airfare Dataset

Develop an automated data collection pipeline that periodically retrieves airfare information for selected flight routes, creating a continuously growing historical dataset suitable for long-term analysis.

### Establish a Scalable Data Pipeline

Design and implement a modular data ingestion and transformation pipeline capable of validating, cleaning, and storing collected flight pricing data using modern data engineering practices.

### Develop a Cloud-Based Data Lake

Store both raw and processed datasets within a private cloud-based data lake to support efficient querying, historical preservation, and future scalability while maintaining data integrity.

### Perform Exploratory Data Analysis

Analyse the collected data to identify pricing trends, seasonal fluctuations, airline comparisons, booking window behaviour, and route-specific characteristics through statistical analysis and visualisation.

### Enable Predictive Analytics

Prepare the dataset for future machine learning applications, including airfare forecasting, price trend prediction, anomaly detection, and booking recommendation models.

### Demonstrate Modern Engineering Practices

Apply industry-standard software engineering and data engineering principles throughout the project, including modular system architecture, automated workflows, version control, cloud-native storage, reproducible data processing, and maintainable code design.

### Support Future Extensibility

Design the system to remain provider-agnostic and extensible, allowing additional flight data providers, destinations, analytical capabilities, and downstream applications to be integrated with minimal architectural changes.

## System Architecture

The Flight Price Observatory adopts a modular, cloud-native architecture designed to automate the collection, storage, processing, and analysis of historical airfare pricing data. Each component has a single responsibility, allowing the system to remain maintainable, extensible, and resilient to changes in upstream data providers.

The system is composed of six primary layers:

### Scheduler Layer

GitHub Actions serves as the orchestration layer responsible for executing scheduled workflows at predefined intervals. Each workflow initiates the data collection process, ensuring that flight pricing information is captured consistently without manual intervention.

Future enhancements may introduce configurable collection frequencies or event-driven execution while maintaining the same pipeline architecture.

---

### Data Collection Layer

The data collection layer is responsible for retrieving flight pricing information from external providers.

Rather than tightly coupling the application to a single provider, the system implements a provider abstraction layer. Each provider adheres to a common interface, allowing new providers to be integrated with minimal changes to the remainder of the system.

This design enables the project to remain resilient should an individual provider become unavailable or change its implementation.

Primary responsibilities include:

- Retrieving flight pricing data
- Standardising provider responses
- Handling request failures and retries
- Rate limiting and request management

---

### Validation & Transformation Layer

Raw provider responses are validated before being transformed into a standardised internal data model.

This stage performs tasks such as:

- Schema validation
- Data type validation
- Duplicate detection
- Data normalisation
- Derived attribute generation
- Metadata enrichment

Maintaining a consistent schema ensures that downstream analytical processes remain independent of provider-specific response formats.

---

### Data Lake Layer

Validated datasets are stored within a private Cloudflare R2 data lake.

The storage architecture follows an immutable design where raw provider responses are preserved separately from processed analytical datasets. This approach allows historical data to be reprocessed should transformation logic change in the future.

The data lake consists of multiple storage tiers:

- Raw JSON datasets
- Processed Parquet datasets
- Analytics-ready datasets

Partitioning datasets by collection date enables efficient querying while reducing unnecessary data movement.

---

### Analytics Layer

DuckDB serves as the analytical query engine responsible for performing SQL-based analysis directly against Parquet datasets stored within the data lake.

This layer supports:

- Exploratory data analysis
- Route comparisons
- Airline comparisons
- Seasonal trend analysis
- Booking window analysis
- Statistical aggregation

Separating analytical workloads from the ingestion pipeline allows each component to evolve independently.

---

### Presentation Layer

Processed analytical datasets are visualised through an interactive dashboard.

The dashboard provides insights into historical airfare behaviour through charts, summary statistics, and trend visualisations while remaining decoupled from the underlying data collection pipeline.

Future iterations may expose analytical datasets through APIs or machine learning services.

---

### High-Level Architecture

```text
                    GitHub Actions
                 (Scheduled Workflow)

                           │

                           ▼

               Flight Provider Interface

                           │

          ┌────────────────┴────────────────┐

          ▼                                 ▼

   Skyscanner Provider              Future Providers

          │

          ▼

      Validation Layer

          │

          ▼

   Transformation Pipeline

          │

          ▼

      Cloudflare R2 Data Lake

          │

    ┌─────┴───────────────┐

    ▼                     ▼

 Raw JSON          Processed Parquet

    └──────────────┬──────────────┘

                   ▼

                DuckDB

                   ▼

          Analytics Dashboard
```

---

## Technology Stack

The project utilises a modern Python-based data engineering stack chosen for its simplicity, scalability, interoperability, and suitability for analytical workloads.

### Programming Language

| Component | Technology   | Purpose                      |
| --------- | ------------ | ---------------------------- |
| Language  | Python 3.12+ | Core application development |

Python provides a mature ecosystem for HTTP communication, data processing, automation, analytics, and machine learning, allowing the entire platform to be developed using a single language.

---

### Workflow Orchestration

| Component | Technology     | Purpose                      |
| --------- | -------------- | ---------------------------- |
| Scheduler | GitHub Actions | Automated workflow execution |

GitHub Actions schedules and executes the data collection pipeline without requiring dedicated infrastructure or self-hosted schedulers.

---

### HTTP Communication

| Component   | Technology | Purpose           |
| ----------- | ---------- | ----------------- |
| HTTP Client | httpx      | API communication |

The HTTP client is responsible for interacting with external flight data providers while supporting modern HTTP features and future asynchronous execution if required.

---

### Data Processing

| Component  | Technology | Purpose                            |
| ---------- | ---------- | ---------------------------------- |
| DataFrames | Polars     | High-performance data manipulation |
| Validation | Pydantic   | Schema validation and type safety  |

Polars performs transformation and aggregation of collected datasets, while Pydantic validates incoming provider responses before storage.

---

### Data Storage

| Component         | Technology    | Purpose                      |
| ----------------- | ------------- | ---------------------------- |
| Raw Format        | JSON          | Immutable provider responses |
| Analytical Format | Parquet       | Optimised analytical storage |
| Data Lake         | Cloudflare R2 | Private object storage       |
| Query Engine      | DuckDB        | SQL analytics over Parquet   |

Raw data is preserved for reproducibility, while processed datasets are stored using the Parquet columnar format to support efficient analytical queries.

---

### Visualisation

| Component | Technology | Purpose                          |
| --------- | ---------- | -------------------------------- |
| Dashboard | Streamlit  | Interactive analytical dashboard |
| Charts    | Plotly     | Interactive data visualisation   |

The presentation layer enables users to explore historical pricing trends through interactive visualisations without directly interacting with the underlying datasets.

---

### Development

| Component          | Technology | Purpose                               |
| ------------------ | ---------- | ------------------------------------- |
| Version Control    | Git        | Source code management                |
| Repository         | GitHub     | Code hosting                          |
| Testing            | pytest     | Automated testing                     |
| Package Management | uv         | Dependency and environment management |

The development toolchain emphasises reproducibility, maintainability, and continuous integration throughout the software development lifecycle.

## Project Structure

The repository is organised into modular components that separate application logic, data processing, storage, documentation, and infrastructure. Each directory has a clearly defined responsibility to improve maintainability, scalability, and ease of navigation.

```text
flight-price-observatory/

├── .github/
│   └── workflows/
│       └── collect-flight-data.yml
│
├── docs/
│   ├── architecture.md
│   ├── design.md
│   ├── data-model.md
│   └── decisions/
│
├── collector/
│   ├── providers/
│   ├── models/
│   ├── services/
│   └── main.py
│
├── pipeline/
│   ├── validation/
│   ├── transformation/
│   └── enrichment/
│
├── storage/
│   ├── r2/
│   └── parquet/
│
├── analytics/
│   ├── queries/
│   ├── notebooks/
│   └── reports/
│
├── dashboard/
│
├── tests/
│
├── pyproject.toml
├── README.md
└── LICENSE
```

### Repository Overview

#### `.github/`

Contains all GitHub Actions workflows responsible for automating scheduled data collection, testing, and future deployment tasks.

Typical responsibilities include:

- Scheduled data collection
- Continuous Integration (CI)
- Automated testing
- Workflow orchestration

---

#### `docs/`

Contains all project documentation.

Documentation is intentionally separated from the source code to improve discoverability and maintain a clear distinction between implementation and design.

Example documents include:

- Software Design Document
- System Architecture
- Data Model
- Development Roadmap
- Architecture Decision Records (ADRs)

---

#### `collector/`

Implements the data collection layer.

This module communicates with external flight data providers and converts provider-specific responses into a standardised internal representation.

Responsibilities include:

- Provider abstraction
- API communication
- Request retries
- Error handling
- Response parsing

The provider abstraction allows additional flight data providers to be integrated without affecting downstream components.

---

#### `pipeline/`

Implements the Extract, Transform, Load (ETL) pipeline.

After data is collected, it is validated, normalised, enriched, and prepared for long-term storage.

Typical processing tasks include:

- Schema validation
- Data cleaning
- Duplicate detection
- Derived attribute generation
- Metadata enrichment
- Dataset transformation

---

#### `storage/`

Provides a storage abstraction over the project's data lake.

This module is responsible for reading from and writing to Cloudflare R2 while hiding storage-specific implementation details from the remainder of the application.

Responsibilities include:

- Uploading raw datasets
- Downloading analytical datasets
- Writing Parquet files
- Managing dataset partitions

---

#### `analytics/`

Contains analytical workloads performed on the historical datasets.

This directory includes reusable SQL queries, exploratory notebooks, and generated reports that operate on processed datasets rather than raw provider responses.

Example analyses include:

- Route comparison
- Airline comparison
- Booking window analysis
- Seasonal trends
- Historical price distributions

---

#### `dashboard/`

Contains the presentation layer of the project.

The dashboard provides an interactive interface for exploring historical airfare data through charts, tables, filters, and summary statistics.

The dashboard remains independent from the ingestion pipeline and consumes only processed analytical datasets.

---

#### `tests/`

Contains automated unit, integration, and future end-to-end tests.

Testing is organised to verify individual modules independently while ensuring the complete data collection pipeline functions correctly.

---

### Root Files

#### `README.md`

Provides a high-level introduction to the project, installation instructions, repository overview, and links to detailed documentation.

---

#### `pyproject.toml`

Defines project metadata, dependencies, build configuration, and development tooling.

---

#### `LICENSE` and `NOTICE`

Specifies the licensing terms governing the source code and repository.

## Getting Started

Follow the steps below to set up the project for local development.

### Prerequisites

Ensure the following software is installed before continuing:

- Python 3.12 or later
- Git
- A GitHub account
- A Cloudflare account with an R2 bucket
- Flight data provider credentials (if required)

### Clone the Repository

```bash
git clone https://github.com/notlega/flight-price-observatory.git

cd flight-price-observatory
```

---

## Installation

This project uses **uv** for dependency management.

Install all project dependencies using:

```bash
uv sync
```

---

## Configuration

Project configuration is managed through environment variables.

Create a local environment file:

```text
.env
```

Example:

```text
FLIGHT_PROVIDER_API_KEY=

R2_ACCESS_KEY=

R2_SECRET_KEY=

R2_BUCKET=

R2_ENDPOINT=

R2_REGION=

COLLECTION_SCHEDULE=
```

The `.env` file should never be committed to source control.

---

## Running Locally

Run a manual collection job:

```bash
uv run python -m scraper.main
```

Run the analytics pipeline:

```bash
uv run python -m pipeline.main
```

Launch the dashboard:

```bash
streamlit run dashboard/app.py
```

Run automated tests:

```bash
pytest
```

---

## Documentation

Additional project documentation is available under the `docs/` directory.

| Document          | Description                           |
| ----------------- | ------------------------------------- |
| `design.md`       | Project overview and design decisions |
| `architecture.md` | System architecture                   |
| `data-model.md`   | Dataset schema                        |
| `decisions/`      | Architecture Decision Records (ADRs)  |

---

## Roadmap

The project is currently under active development.

Planned milestones include:

- Build provider abstraction layer
- Implement automated data collection
- Integrate Cloudflare R2 data lake
- Develop ETL pipeline
- Build analytical data model
- Develop interactive dashboard
- Implement statistical analysis
- Explore predictive machine learning models
- Improve monitoring and observability
- Support additional flight data providers

Future milestones may evolve as project requirements change.

---

## Contributing

Contributions are welcome.

Before submitting a contribution, please:

1. Open an issue describing the proposed change.
2. Discuss significant architectural changes before implementation.
3. Create a feature branch from the latest main branch.
4. Ensure all tests pass before submitting a pull request.
5. Follow the project's coding conventions and documentation standards.

For major architectural decisions, contributors are encouraged to document the rationale using an Architecture Decision Record (ADR) within the `docs/decisions/` directory.

---

## License

This project is licensed under the terms of the license contained in the `LICENSE` file.

By contributing to this repository, contributors agree that their contributions will be licensed under the same terms unless explicitly agreed otherwise.

---

## Notice

Additional copyright notices, acknowledgements, and attribution information are provided in the `NOTICE` file where applicable.
