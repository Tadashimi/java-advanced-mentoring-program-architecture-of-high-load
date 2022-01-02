--Create table script
create table ads_data1
(
    date                  Date,
    time                  DateTime,
    event                 LowCardinality(String),
    platform              LowCardinality(String),
    ad_id                 UInt32,
    client_union_id       UInt32,
    campaign_union_id     UInt32,
    ad_cost_type          LowCardinality(String),
    ad_cost               Float32,
    has_video             Int8,
    target_audience_count UInt64
)
    engine = MergeTree PARTITION BY date ORDER BY (time, ad_id) SAMPLE BY ad_id SETTINGS index_granularity = 8192;