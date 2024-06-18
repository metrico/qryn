export interface Row {
    trace_id: string;
    span_id: string[];
    duration: string[];
    timestamp_ns: string[];
    start_time_unix_nano: string;
    duration_ms: number;
    root_service_name: string;
    payload: string[];
    payload_type: number[];
}