from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration


topic_name = 'green-trips'

def create_events_aggregated_sink(t_env):
    """Create PostgreSQL sink table for storing aggregated streaming results"""
    # Define destination table for session-windowed aggregation results
    table_name = 'processed_greentrips_aggregated'
    # SQL DDL to create JDBC sink for aggregated data
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            trip_count BIGINT,
            avg_trip_distance FLOAT,
            total_trip_distance FLOAT
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
            lpep_dropoff_datetime STRING,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance FLOAT,
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


def log_aggregation():
    """Main streaming aggregation job that computes windowed statistics"""
    # Set up the execution environment
    # Create PyFlink streaming environment for distributed aggregation processing
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Enable fault tolerance with 10-second checkpoints
    env.set_parallelism(1)

    # Set up the table environment
    # Configure table environment for SQL-based streaming aggregations
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
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
            PULocationID,
            DOLocationID,
            SESSION_START(event_watermark, INTERVAL '5' MINUTES) AS session_start,
            SESSION_END(event_watermark, INTERVAL '5' MINUTES) AS session_end,
            COUNT(*) AS trip_count,
            AVG(trip_distance) AS avg_trip_distance,
            SUM(trip_distance) AS total_trip_distance
        FROM {source_table}
        GROUP BY PULocationID, DOLocationID, SESSION(event_watermark, INTERVAL '5' MINUTES)
        """).wait()  # Wait for streaming aggregation job to complete

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    # Start the streaming aggregation pipeline
    log_aggregation()
