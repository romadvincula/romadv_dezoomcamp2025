from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_aggregated_sink(t_env):
    """Create PostgreSQL sink table for storing aggregated streaming results"""
    # Define destination table for time-windowed aggregation results
    table_name = 'processed_events_aggregated'
    # SQL DDL to create JDBC sink with composite primary key for aggregated data
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP(3),
            test_data INT,
            num_hits BIGINT,
            PRIMARY KEY (event_hour, test_data) NOT ENFORCED
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
    """Create Kafka source table for reading streaming data with watermarks"""
    # Define source table for streaming events with event-time processing
    table_name = "events"
    # SQL DDL to create Kafka source with watermarks for handling late-arriving data
    source_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,
            event_timestamp BIGINT,
            event_watermark AS TO_TIMESTAMP_LTZ(event_timestamp, 3),
            WATERMARK for event_watermark as event_watermark - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'test-topic',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_aggregation():
    """Main streaming aggregation job that computes windowed statistics"""
    # Set up the execution environment
    # Create PyFlink streaming environment for distributed aggregation processing
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Enable fault tolerance with 10-second checkpoints
    env.set_parallelism(3)  # Use 3 parallel tasks for distributed processing

    # Set up the table environment
    # Configure table environment for SQL-based streaming aggregations
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Configure watermark strategy for handling out-of-order streaming events
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))  # Allow 5 seconds for late data
        .with_timestamp_assigner(
            # This lambda is your timestamp assigner:
            #   event -> The data record
            #   timestamp -> The previously assigned (or default) timestamp
            lambda event, timestamp: event[2]  # We treat the second tuple element as the event-time (ms).
        )
    )
    try:
        # Create Kafka table
        # Set up streaming data source from Kafka topic
        source_table = create_events_source_kafka(t_env)
        # Set up streaming aggregation sink to PostgreSQL database
        aggregated_table = create_events_aggregated_sink(t_env)

        # Execute streaming SQL query with tumbling window aggregation
        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start as event_hour,
            test_data,
            COUNT(*) AS num_hits
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_watermark), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, test_data;
        
        """).wait()  # Wait for streaming aggregation job to complete

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    # Start the streaming aggregation pipeline
    log_aggregation()
