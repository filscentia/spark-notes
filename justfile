# Main justfile to run all the development scripts
# To install 'just' see https://github.com/casey/just#installation

# Ensure all properties are exported as shell env-vars
set export
set dotenv-load

# set the current directory, and the location of the test dats
CWDIR := justfile_directory()

SPARK_VERSION := "3.5.8"

SPARK_MASTER_CONTAINER := "containers-spark-1"

_default:
  @just -f {{justfile()}} --list

# Builds both Java applications into JAR files
build-java-app:
    #!/bin/bash
    set -e -o pipefail -x

    # Build hello_java_app
    echo "Building hello_java_app..."
    pushd ${CWDIR}/hello_java_app/
    ./gradlew build
    popd
    cp ${CWDIR}/hello_java_app/build/libs/hello_java_app-*-all.jar ${CWDIR}/containers/_apps/hello_java_app.jar

    # Build load_data_java_app
    echo "Building load_data_java_app..."
    pushd ${CWDIR}/load_data_java_app/
    ./gradlew build
    popd
    cp ${CWDIR}/load_data_java_app/build/libs/java_app-*-all.jar ${CWDIR}/containers/_apps/load_data_java_app.jar

    echo "✓ Both Java applications built successfully"


java-app:
    #!/bin/bash
    set -e -o pipefail -x
    docker exec -it ${SPARK_MASTER_CONTAINER} bash -c "/opt/spark/bin/spark-submit --master spark://${SPARK_MASTER_CONTAINER}:7077 --driver-memory 1G --executor-memory 1G /opt/spark-apps/app.jar"

# Loads the parquet data files into Spark tables
load-data:
    #!/bin/bash
    set -e -o pipefail
    docker exec -it ${SPARK_MASTER_CONTAINER} bash -c "/opt/spark/bin/spark-submit --master spark://${SPARK_MASTER_CONTAINER}:7077 --driver-memory 1G --executor-memory 1G /opt/spark-apps/load_parquet_tables.py 2>/dev/null"

# Displays information about existing Spark tables
show-table-info:
    #!/bin/bash
    set -e -o pipefail
    docker exec -it ${SPARK_MASTER_CONTAINER} bash -c "/opt/spark/bin/spark-submit --master spark://${SPARK_MASTER_CONTAINER}:7077 --driver-memory 1G --executor-memory 1G /opt/spark-apps/show_table_info.py 2>/dev/null"

hello:
    #!/bin/bash
    set -e -o pipefail
    docker exec -it ${SPARK_MASTER_CONTAINER} bash -c "/opt/spark/bin/spark-submit --master spark://${SPARK_MASTER_CONTAINER}:7077  --driver-memory 1G --executor-memory 1G /opt/spark-apps/hello_spark.py "

# Cleans up metastore and warehouse contents while preserving directories
clean-spark-data:
    #!/bin/bash
    set -e -o pipefail
    echo "Cleaning Spark metastore and warehouse..."
    sudo rm -rf ${CWDIR}/_metastore/*
    sudo rm -rf ${CWDIR}/_warehouse/*


# Executes a Spark application using JSON configuration
# Usage: just run-spark-app <json_file> [app_name] [main_class] [jar_path]
run-spark-app json_file app_name="MyApp" main_class="com.example.Main" jar_path="/opt/spark-apps/app.jar":
    #!/bin/bash
    set -e -o pipefail
    
    # Function to build spark-submit command from JSON
    build_spark_submit_cmd() {
        local json_file="$1"
        local app_name="$2"
        local main_class="$3"
        local jar_path="$4"
        
        if [ ! -f "$json_file" ]; then
            echo "Error: JSON file not found: $json_file" >&2
            exit 1
        fi
        
        # Check if jq is available
        if ! command -v jq &> /dev/null; then
            echo "Error: jq is required but not installed" >&2
            exit 1
        fi
        
        # Export variables for substitution
        export APP_NAME="$app_name"
        export APP_MAIN_CLASS="$main_class"
        export APP_JAR_S3="$jar_path"
        
        # Parse JSON and build command
        local cmd="/opt/spark/bin/spark-submit"
        cmd="$cmd --master spark://${SPARK_MASTER_CONTAINER}:7077"
        cmd="$cmd --driver-memory 1G"
        cmd="$cmd --executor-memory 1G"
        
        # Add conf parameters
        local conf_params=$(jq -r '.application_details.conf | to_entries[] | "--conf \(.key)=\(.value)"' "$json_file" | envsubst)
        while IFS= read -r conf; do
            cmd="$cmd $conf"
        done <<< "$conf_params"
        
        # Add class
        local class=$(jq -r '.application_details.class' "$json_file" | envsubst)
        cmd="$cmd --class $class"
        
        # Add application jar
        local app_jar=$(jq -r '.application_details.application' "$json_file" | envsubst)
        cmd="$cmd $app_jar"
        
        # Add arguments
        local args=$(jq -r '.application_details.arguments[]' "$json_file")
        while IFS= read -r arg; do
            cmd="$cmd \"$arg\""
        done <<< "$args"
        
        echo "$cmd"
    }
    
    # Build the command
    SPARK_CMD=$(build_spark_submit_cmd "{{json_file}}" "{{app_name}}" "{{main_class}}" "{{jar_path}}")
    
    echo "Executing Spark application..."
    echo "Command: $SPARK_CMD"
    echo ""
    
    # Execute the command in the Spark container
    docker exec -it ${SPARK_MASTER_CONTAINER} bash -c "$SPARK_CMD"

    rm -f ${CWDIR}/.last_spark_version
    echo "✓ Metastore and warehouse cleaned successfully"

# Starts a simple Spark cluster locally in docker
spark ver:
    #!/bin/bash
    set -e -o pipefail

    declare -A spark_container_images
    spark_container_images=(["3.4.4"]="3.4.4-scala2.12-java11-python3-ubuntu"  ["3.5.8"]="3.5.8-scala2.12-java17-python3-ubuntu" ["4.0.2"]="4.0.2-scala2.13-java17-python3-ubuntu" )

    # Function to normalize version aliases
    normalize_version() {
        case "$1" in
            34|3.4) echo "3.4.4" ;;
            35|3.5) echo "3.5.8" ;;
            40|4.0) echo "4.0.2" ;;
            *) echo "$1" ;;
        esac
    }

    # Normalize and validate supplied version
    CURRENT_VERSION=$(normalize_version "{{ver}}")
    if [ -z "${spark_container_images[$CURRENT_VERSION]}" ]; then
        echo ""
        echo "╔═══════════════════════════════════════════════════════════════════╗"
        echo "║  ❌ ERROR: Invalid Spark version '$CURRENT_VERSION'                      ║"
        echo "║  Valid versions: 3.4.4, 3.5.8, 4.0.2                             ║"
        echo "║  Aliases: 34/3.4 → 3.4.4, 35/3.5 → 3.5.8, 40/4.0 → 4.0.2        ║"
        echo "║  Usage: just spark <version>                                     ║"
        echo "╚═══════════════════════════════════════════════════════════════════╝"
        echo ""
        exit 1
    fi

    # Track last Spark version used
    LAST_VERSION_FILE="${CWDIR}/.last_spark_version"

    if [ -f "$LAST_VERSION_FILE" ]; then
        LAST_VERSION=$(cat "$LAST_VERSION_FILE")
        if [ "$LAST_VERSION" != "$CURRENT_VERSION" ]; then
            echo ""
            echo "╔═══════════════════════════════════════════════════════════════════╗"
            echo "║  ⚠️  WARNING: Spark version changed from $LAST_VERSION to $CURRENT_VERSION           ║"
            echo "║  Metastore/warehouse may need cleanup for compatibility.          ║"
            echo "║  Consider running: just clean-spark-data                          ║"
            echo "╚═══════════════════════════════════════════════════════════════════╝"
            echo ""
            read -p "Press Enter to continue or Ctrl+C to abort..."
        fi
    fi

    # Record current version
    echo "$CURRENT_VERSION" > "$LAST_VERSION_FILE"

    export MY_UID=$(id -u)
    export MY_GID=$(id -g)
    export SPARK_CONTAINER_IMAGE=${spark_container_images[{{ver}}]}

    docker compose up


# Generates a spark-submit command from JSON application details
# Usage: just spark-submit-from-json <json_file> [app_name] [main_class] [jar_path]
spark-submit-from-json json_file app_name="MyApp" main_class="com.example.Main" jar_path="/opt/spark-apps/app.jar":
    #!/bin/bash
    set -e -o pipefail
    
    # Function to build spark-submit command from JSON
    build_spark_submit_cmd() {
        local json_file="$1"
        local app_name="$2"
        local main_class="$3"
        local jar_path="$4"
        
        if [ ! -f "$json_file" ]; then
            echo "Error: JSON file not found: $json_file" >&2
            exit 1
        fi
        
        # Check if jq is available
        if ! command -v jq &> /dev/null; then
            echo "Error: jq is required but not installed" >&2
            exit 1
        fi
        
        # Export variables for substitution
        export APP_NAME="$app_name"
        export APP_MAIN_CLASS="$main_class"
        export APP_JAR_S3="$jar_path"
        
        # Parse JSON and build command
        local cmd="/opt/spark/bin/spark-submit"
        cmd="$cmd --master spark://${SPARK_MASTER_CONTAINER}:7077"
        cmd="$cmd --driver-memory 1G"
        cmd="$cmd --executor-memory 1G"
        
        # Add conf parameters
        local conf_params=$(jq -r '.application_details.conf | to_entries[] | "--conf \(.key)=\(.value)"' "$json_file" | envsubst)
        while IFS= read -r conf; do
            cmd="$cmd $conf"
        done <<< "$conf_params"
        
        # Add class
        local class=$(jq -r '.application_details.class' "$json_file" | envsubst)
        cmd="$cmd --class $class"
        
        # Add application jar
        local app_jar=$(jq -r '.application_details.application' "$json_file" | envsubst)
        cmd="$cmd $app_jar"
        
        # Add arguments
        local args=$(jq -r '.application_details.arguments[]' "$json_file")
        while IFS= read -r arg; do
            cmd="$cmd \"$arg\""
        done <<< "$args"
        
        echo "$cmd"
    }
    
    # Build and display the command
    SPARK_CMD=$(build_spark_submit_cmd "{{json_file}}" "{{app_name}}" "{{main_class}}" "{{jar_path}}")
    echo "Generated spark-submit command:"
    echo "$SPARK_CMD"
    echo ""
    echo "To execute, run:"
    echo "docker exec -it ${SPARK_MASTER_CONTAINER} bash -c \"$SPARK_CMD\""

