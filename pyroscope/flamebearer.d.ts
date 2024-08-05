
type int64 = string;
type uint64 = string;
type units = string;

export interface Flamebearer {
    version: number,
    flamebearerProfileV1: flamebearerProfileV1
    telemetry?: {[key: string]: any}
}

export interface flamebearerProfileV1 {
    flamebearer: flamebearerV1,
    metadata: flamebearerMetadataV1,
    timeline: flamebearerTimelineV1,
    groups: {[key: string]: flamebearerTimelineV1}
    heatmap: heatmap,
    leftTicks: string,
    rightTicks: string,
}

export interface flamebearerV1 {
    names: string,
    levels: [[number]],
    numTicks: number,
    maxSelf: number
}

export interface flamebearerMetadataV1 {
    format: string,
    spyName: string,
    sampleRate: number,
    units: units,
    name: string
}

export interface flamebearerTimelineV1 {
    startTime: int64,
    samples: [uint64]
    durationDelta: int64,
    watermarks: {[key: number]: int64}
}

export interface heatmap {
    values: [[uint64]],
    timeBuckets: int64,
    valueBuckets: int64,
    startTime: int64,
    endTime: int64,
    minValue: uint64,
    maxValue: uint64,
    minDepth: uint64,
    maxDepth: uint64
}
