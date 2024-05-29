-- TODO: parameterize?
with
    es as (
        select * from system.text_log order by event_time_microseconds desc limit 2000
    ),
    queries as (
        select * from system.query_log
        where type = 'QueryFinish'
    )
select
    es.query_id as query_id,
    any(query_log.initial_query_id) as initial_query_id,
    any(query_log.query) as query,
    any(if(query_log.is_initial_query, '', initial_query_log.query)) as initial_query,
    any(query_log.ProfileEvents) as query_profile_events,
    groupUniqArray(es.logger_name) as logger_names,
    groupArray(
        tuple(
            es.event_time_microseconds,
            es.level,
            es.thread_name,
            es.logger_name,
            es.message,
            es.hostname,
            es.revision,
            es.source_file,
            es.source_line
        )::Tuple( -- will be unneeded once this is merged: https://github.com/ClickHouse/ClickHouse/pull/54881
            event_time DateTime64,
            level String,
            thread_name String,
            logger_name String,
            message String,
            hostname String,
            revision String,
            source_file String,
            source_line UInt64
        ) as event_tuple
    ) as events,
    groupArrayMap(map(es.logger_name, event_tuple)) as events_by_logger,
    groupArray(tuple(query_log.*)) as ql
    -- this might help, but the problem is the JSON gets escaped.. so maybe the final output needs to be a manually constructed json object. ugh.
    -- groupArray(formatRowNoNewline('JSONEachRow', query_log.*)) as ql_json
from es 
left any join queries as query_log on es.query_id = query_log.query_id
left any join queries as initial_query_log on query_log.initial_query_id = initial_query_log.query_id
where query_log.query != ''
group by es.query_id
FORMAT JSON
SETTINGS
allow_experimental_analyzer=1
