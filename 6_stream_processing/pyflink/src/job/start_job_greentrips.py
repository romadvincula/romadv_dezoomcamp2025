from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment


topic_name = 'green-trips'
sink_table_name = 'processed_greentrips'

def create_processed_events_sink_postgres(t_env):
    """Create PostgreSQL sink table for storing processed streaming data"""
    # Define destination table for processed streaming results
    table_name = sink_table_name
    # SQL DDL to create JDBC sink connector for database storage
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count STRING,
            trip_distance FLOAT,
            tip_amount FLOAT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    """Create Kafka source table for reading streaming data"""
    # Define source table for incoming streaming data using PyFlink
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    # SQL DDL to create Kafka source connector with watermarks for event time processing
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count STRING,
            trip_distance FLOAT,
            tip_amount FLOAT,
            event_watermark AS TO_TIMESTAMP(CAST(lpep_dropoff_datetime AS STRING), 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK for event_watermark as event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = '{topic_name}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    """Main streaming job that processes data from Kafka to PostgreSQL"""
    # Set up the execution environment
    # Create PyFlink streaming execution environment for distributed processing
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Enable fault tolerance with 10-second checkpoints
    # env.set_parallelism(1)

    # Set up the table environment
    # Configure table environment for SQL-based streaming operations
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        # Set up streaming data source from Kafka topic
        source_table = create_events_source_kafka(t_env)
        # Set up streaming data sink to PostgreSQL database
        postgres_sink = create_processed_events_sink_postgres(t_env)
        # write records to postgres too!
        # Execute streaming SQL query to transform and move data from Kafka to PostgreSQL
        t_env.execute_sql(
            f"""
                INSERT INTO {postgres_sink}
                SELECT
                    lpep_pickup_datetime,
                    lpep_dropoff_datetime,
                    PULocationID,
                    DOLocationID,
                    passenger_count,
                    trip_distance,
                    tip_amount
                FROM {source_table}
                    """
        ).wait()  # Wait for streaming job to complete

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    # Start the streaming data processing pipeline
    log_processing()
