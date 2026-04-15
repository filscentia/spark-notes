# Spark Notes

A development environment for Apache Spark with Docker, supporting multiple Spark versions and Java/Python applications.

## Prerequisites

- Docker and Docker Compose
- [just](https://github.com/casey/just#installation) command runner
- `jq` (for JSON-based application execution)

## Quick Start

```bash
# Start Spark 3.5 cluster
just spark 3.5

# Load sample data
just load-data

# Show table information
just show-table-info
```

## Spark Version Management

### Starting Spark Clusters

Start a Spark cluster with version aliases or full versions:

```bash
# Using short aliases
just spark 34    # Spark 3.4.4
just spark 35    # Spark 3.5.8
just spark 40    # Spark 4.0.2

# Using version prefixes
just spark 3.4   # Spark 3.4.4
just spark 3.5   # Spark 3.5.8
just spark 4.0   # Spark 4.0.2

# Using full versions
just spark 3.4.4
just spark 3.5.8
just spark 4.0.2
```

### Cleaning Data Between Versions

When switching between Spark versions, clean the metastore and warehouse to avoid compatibility issues:

```bash
just clean-spark-data
```

This removes all data from `_metastore/` and `_warehouse/` directories while preserving the directory structure.

## Building Applications

### Build Java Applications

Build both Java applications and copy JARs to the containers directory:

```bash
just build-java-app
```

This builds:
- `hello_java_app` → `containers/_apps/hello_java_app.jar`
- `load_data_java_app` → `containers/_apps/load_data_java_app.jar`

## Running Applications

### Python Applications

```bash
# Hello World example
just hello

# Load parquet data into tables
just load-data

# Display table information
just show-table-info
```

### Java Applications

```bash
# Run Java application (legacy)
just java-app
```

### JSON-Based Application Execution

Execute Spark applications using JSON configuration files for complex setups:

```bash
# Dry run - see generated spark-submit command
just spark-submit-from-json containers/example_app_config.json \
    "VehicleAnalysis" \
    "com.filscentia.sparknotes.VehicleApp" \
    "/opt/spark-apps/load_data_java_app.jar"

# Execute application
just run-spark-app containers/example_app_config.json \
    "VehicleAnalysis" \
    "com.filscentia.sparknotes.VehicleApp" \
    "/opt/spark-apps/load_data_java_app.jar"
```

**JSON Configuration Format:**

```json
{
   "application_details":{  
        "conf": { 
            "spark.sql.defaultCatalog":"spark_catalog",
            "spark.app.name" : "$APP_NAME"
        },
        "class":"$APP_MAIN_CLASS",
        "application": "$APP_JAR_S3",
        "arguments":["arg1", "arg2"] 
     }      
}
```

See `containers/README_SPARK_JSON.md` for detailed documentation.

## Available Commands

List all available commands:

```bash
just
```

### Core Commands

- `just spark <version>` - Start Spark cluster (supports aliases: 34, 35, 40, 3.4, 3.5, 4.0)
- `just build-java-app` - Build both Java applications
- `just clean-spark-data` - Clean metastore and warehouse data

### Application Commands

- `just hello` - Run hello world Python example
- `just load-data` - Load parquet files into Spark tables
- `just show-table-info` - Display information about existing tables
- `just java-app` - Run Java application (legacy)

### JSON-Based Execution

- `just spark-submit-from-json <json> [name] [class] [jar]` - Generate spark-submit command
- `just run-spark-app <json> [name] [class] [jar]` - Execute Spark application from JSON config

## Project Structure

```
.
├── justfile                      # Command definitions
├── containers/
│   ├── docker-compose.yaml       # Docker configuration
│   ├── example_app_config.json   # Example JSON config
│   ├── README_SPARK_JSON.md      # JSON config documentation
│   ├── _apps/                    # Application JARs
│   ├── _data/                    # Sample data files
│   ├── _logs/                    # Spark logs
│   ├── _metastore/               # Hive metastore
│   └── _warehouse/               # Spark warehouse
├── hello_java_app/               # Hello World Java app
├── load_data_java_app/           # Data loading Java app
├── hello_python_app/             # Hello World Python app
├── load_data_python_app/         # Data loading Python app
└── show_tables_python_app/       # Table info Python app
```

## Tips

- Always run `just clean-spark-data` when switching between major Spark versions
- Use version aliases (34, 35, 40) for quick cluster starts
- Build Java apps before running them with `just build-java-app`
- Check `just` (no arguments) to see all available commands
- Use JSON configs for complex application configurations with multiple parameters