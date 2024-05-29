--select groupUniqArray(thread_name), sum(query_duration_ms), any(query) from system.query_thread_log group by normalized_query_hash order by sum(query_duration_ms) asc;

-- rough idea is - take this query and make it parametric
-- - taking a beginning time to filter by
-- output its result on every iteration of benchmark - maybe also write to json file?
-- could this be useful in json traversal context?
with queries_by_normalized_query_hash as (select *,
                                                 row_number() over (partition by normalized_query_hash order by query_start_time_microseconds) as row_number
                                          from cluster('default', 'system', 'query_log')
                                          where query_duration_ms > 0
                                            and type = 'QueryFinish'
                                          order by query_start_time_microseconds),
     (k, v) -> (
        multiIf(
            like(k, '%Bytes%'), formatReadableSize(v),
            like(k, '%Microseconds%'), formatReadableTimeDelta(v::Float64 / 1_000_000.0, 'seconds', 'nanoseconds'),
            like(k, '%Milliseconds%'), formatReadableTimeDelta(v::Float64 / 1_000.0, 'seconds', 'milliseconds'),
            -- TODO microseconds
            formatReadableQuantity(v))
    ) as bestFormattedValue,
     (v, row_number) -> (
        tuple(
            sparkbar(20)(row_number, v),
            argMax(v, row_number) as most_recent,
            sum(v),
            avg(v),
            exponentialMovingAverage(5)(v, row_number),
            (min(v), max(v)),
            simpleLinearRegression(row_number, v),
            groupArray(v),
            groupArrayMap(map(hostname, v))
        )::Tuple(
            graph String,
            most_recent Float64,
           -- skew / curtosis would be cool
            total Float64,
            average Float64,
            moving_average Float64,
            min_max Tuple(min Float64, max Float64),
            -- k is slope, b is offset
            line_of_best_fit Tuple(k Float64, b Float64),
            values Array(Float64),
            values_by_hostname Map(String, Array(Float64))
        )
     ) as measure
select 
    any(formatted_query)                                  as query,
    -- hostname,
    groupUniqArray(hostname)                                  as hostnames,
    count(*)                                              as count,
    measure(query_duration_ms, row_number)                as duration_ms,
    measure(read_rows, row_number)                        as read_rows,
    measure(read_bytes, row_number)                       as read_bytes,
    measure(written_rows, row_number)                     as written_rows,
    measure(written_bytes, row_number)                    as written_bytes,
    measure(memory_usage, row_number)                     as memory_usage,
    measure(peak_threads_usage, row_number)               as peak_threads_usage,
    -- would be nice if this were a map but couldn't find a way to sort the map correctly
    arraySort(e -> -e.outlier_ratio,
        -- TODO: filter these so sparkbar is unbroken with voids
        arrayMap(kv ->
            (
                kv.1 as measurement,
                arrayReduce('sparkbar(20)', kv.2, arrayEnumerate(kv.2)) as graph,
                arrayReduce('sparkbar(20)', outliers::Array(UInt64), arrayEnumerate(kv.2)) as outlier_graph,
                kv.2[length(kv.2)] as most_recent,
                bestFormattedValue(kv.1, most_recent) as formatted_recent,
                arraySum(kv.2) as sum,
                bestFormattedValue(kv.1, sum) as formatted_sum,
                arrayAvg(kv.2) as avg,
                bestFormattedValue(kv.1, avg) as formatted_avg,
                (
                    arrayMin(kv.2),
                    arrayMax(kv.2)
                ) as min_max_value,
                arrayReduce('simpleLinearRegression', arrayEnumerate(kv.2), kv.2) as line_of_best_fit,
                if(length(kv.2) > 0,
                    arrayCount(x -> x > 0,
                        if(length(kv.2) > 3, seriesOutliersDetectTukey(kv.2), arrayWithConstant(length(kv.2), 0)) as outliers
                    ) / length(kv.2), 0) * 100.0 as outlier_percentage,
                    arrayMax(arrayMap(v -> abs(v), outliers)) as max_outlier_score,
                    arrayZip(kv.2::Array(Float64), outliers) as values
            )::Tuple(
                measurement String,
                graph String,
                outlier_graph String,
                most_recent Float64,
                formatted_most_recent String,
                sum Float64,
                formatted_sum String,
                avg Float64,
                formatted_avg String,
                min_max_value Tuple(min Float64, max Float64),
                line_of_best_fit Tuple(k Float64, b Float64),
                outlier_ratio Float64,
                max_outlier_score Int64,
                values Array(Tuple(value Float64, outlier_score Int64))
            ),
            arrayZip(
                mapKeys(groupArrayMap(ProfileEvents) as e),
                mapValues(e)
            )
        )
    )                                      as ProfileEventMeasurements,
    -- TODO: can I clean this up and add more metadata?
    -- TODO: fix duplicates
    (
        groupUniqArray(query_kind)::Array(String),
        arrayFlatten(groupUniqArray(databases))::Array(String),
        arrayFlatten(groupUniqArray(tables))::Array(String),
        arrayFlatten(groupUniqArray(columns))::Array(String),
        arrayFlatten(groupUniqArray(partitions))::Array(String),
        arrayFlatten(groupUniqArray(projections))::Array(String),
        arrayFlatten(groupUniqArray(views))::Array(String),
    )::Tuple(
        query_kind Array(String),
        databases Array(String),
        tables Array(String),
        columns Array(String),
        engines Array(String),
        formats Array(String),
        views Array(String)
    ) as query_metadata
from queries_by_normalized_query_hash
where
    query_start_time > NOW() - INTERVAL 1 HOUR
    and query_kind not in ('Drop')
-- TODO: make hostname grouping optional
group by normalized_query_hash-- , hostname
order by duration_ms.moving_average
        desc
    settings
    allow_experimental_analyzer = 1
format JSON
