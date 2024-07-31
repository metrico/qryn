// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var querier_pb = require('./querier_pb.js');
var google_v1_profile_pb = require('./google/v1/profile_pb.js');
var types_v1_types_pb = require('./types/v1/types_pb.js');

function serialize_google_v1_Profile(arg) {
  if (!(arg instanceof google_v1_profile_pb.Profile)) {
    throw new Error('Expected argument of type google.v1.Profile');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_google_v1_Profile(buffer_arg) {
  return google_v1_profile_pb.Profile.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_AnalyzeQueryRequest(arg) {
  if (!(arg instanceof querier_pb.AnalyzeQueryRequest)) {
    throw new Error('Expected argument of type querier.v1.AnalyzeQueryRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_AnalyzeQueryRequest(buffer_arg) {
  return querier_pb.AnalyzeQueryRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_AnalyzeQueryResponse(arg) {
  if (!(arg instanceof querier_pb.AnalyzeQueryResponse)) {
    throw new Error('Expected argument of type querier.v1.AnalyzeQueryResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_AnalyzeQueryResponse(buffer_arg) {
  return querier_pb.AnalyzeQueryResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_DiffRequest(arg) {
  if (!(arg instanceof querier_pb.DiffRequest)) {
    throw new Error('Expected argument of type querier.v1.DiffRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_DiffRequest(buffer_arg) {
  return querier_pb.DiffRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_DiffResponse(arg) {
  if (!(arg instanceof querier_pb.DiffResponse)) {
    throw new Error('Expected argument of type querier.v1.DiffResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_DiffResponse(buffer_arg) {
  return querier_pb.DiffResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_ProfileTypesRequest(arg) {
  if (!(arg instanceof querier_pb.ProfileTypesRequest)) {
    throw new Error('Expected argument of type querier.v1.ProfileTypesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_ProfileTypesRequest(buffer_arg) {
  return querier_pb.ProfileTypesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_ProfileTypesResponse(arg) {
  if (!(arg instanceof querier_pb.ProfileTypesResponse)) {
    throw new Error('Expected argument of type querier.v1.ProfileTypesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_ProfileTypesResponse(buffer_arg) {
  return querier_pb.ProfileTypesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SelectMergeProfileRequest(arg) {
  if (!(arg instanceof querier_pb.SelectMergeProfileRequest)) {
    throw new Error('Expected argument of type querier.v1.SelectMergeProfileRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SelectMergeProfileRequest(buffer_arg) {
  return querier_pb.SelectMergeProfileRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SelectMergeSpanProfileRequest(arg) {
  if (!(arg instanceof querier_pb.SelectMergeSpanProfileRequest)) {
    throw new Error('Expected argument of type querier.v1.SelectMergeSpanProfileRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SelectMergeSpanProfileRequest(buffer_arg) {
  return querier_pb.SelectMergeSpanProfileRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SelectMergeSpanProfileResponse(arg) {
  if (!(arg instanceof querier_pb.SelectMergeSpanProfileResponse)) {
    throw new Error('Expected argument of type querier.v1.SelectMergeSpanProfileResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SelectMergeSpanProfileResponse(buffer_arg) {
  return querier_pb.SelectMergeSpanProfileResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SelectMergeStacktracesRequest(arg) {
  if (!(arg instanceof querier_pb.SelectMergeStacktracesRequest)) {
    throw new Error('Expected argument of type querier.v1.SelectMergeStacktracesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SelectMergeStacktracesRequest(buffer_arg) {
  return querier_pb.SelectMergeStacktracesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SelectMergeStacktracesResponse(arg) {
  if (!(arg instanceof querier_pb.SelectMergeStacktracesResponse)) {
    throw new Error('Expected argument of type querier.v1.SelectMergeStacktracesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SelectMergeStacktracesResponse(buffer_arg) {
  return querier_pb.SelectMergeStacktracesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SelectSeriesRequest(arg) {
  if (!(arg instanceof querier_pb.SelectSeriesRequest)) {
    throw new Error('Expected argument of type querier.v1.SelectSeriesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SelectSeriesRequest(buffer_arg) {
  return querier_pb.SelectSeriesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SelectSeriesResponse(arg) {
  if (!(arg instanceof querier_pb.SelectSeriesResponse)) {
    throw new Error('Expected argument of type querier.v1.SelectSeriesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SelectSeriesResponse(buffer_arg) {
  return querier_pb.SelectSeriesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SeriesRequest(arg) {
  if (!(arg instanceof querier_pb.SeriesRequest)) {
    throw new Error('Expected argument of type querier.v1.SeriesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SeriesRequest(buffer_arg) {
  return querier_pb.SeriesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_querier_v1_SeriesResponse(arg) {
  if (!(arg instanceof querier_pb.SeriesResponse)) {
    throw new Error('Expected argument of type querier.v1.SeriesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_querier_v1_SeriesResponse(buffer_arg) {
  return querier_pb.SeriesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_types_v1_GetProfileStatsRequest(arg) {
  if (!(arg instanceof types_v1_types_pb.GetProfileStatsRequest)) {
    throw new Error('Expected argument of type types.v1.GetProfileStatsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_types_v1_GetProfileStatsRequest(buffer_arg) {
  return types_v1_types_pb.GetProfileStatsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_types_v1_GetProfileStatsResponse(arg) {
  if (!(arg instanceof types_v1_types_pb.GetProfileStatsResponse)) {
    throw new Error('Expected argument of type types.v1.GetProfileStatsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_types_v1_GetProfileStatsResponse(buffer_arg) {
  return types_v1_types_pb.GetProfileStatsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_types_v1_LabelNamesRequest(arg) {
  if (!(arg instanceof types_v1_types_pb.LabelNamesRequest)) {
    throw new Error('Expected argument of type types.v1.LabelNamesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_types_v1_LabelNamesRequest(buffer_arg) {
  return types_v1_types_pb.LabelNamesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_types_v1_LabelNamesResponse(arg) {
  if (!(arg instanceof types_v1_types_pb.LabelNamesResponse)) {
    throw new Error('Expected argument of type types.v1.LabelNamesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_types_v1_LabelNamesResponse(buffer_arg) {
  return types_v1_types_pb.LabelNamesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_types_v1_LabelValuesRequest(arg) {
  if (!(arg instanceof types_v1_types_pb.LabelValuesRequest)) {
    throw new Error('Expected argument of type types.v1.LabelValuesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_types_v1_LabelValuesRequest(buffer_arg) {
  return types_v1_types_pb.LabelValuesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_types_v1_LabelValuesResponse(arg) {
  if (!(arg instanceof types_v1_types_pb.LabelValuesResponse)) {
    throw new Error('Expected argument of type types.v1.LabelValuesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_types_v1_LabelValuesResponse(buffer_arg) {
  return types_v1_types_pb.LabelValuesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


var QuerierServiceService = exports.QuerierServiceService = {
  // ProfileType returns a list of the existing profile types.
profileTypes: {
    path: '/querier.v1.QuerierService/ProfileTypes',
    requestStream: false,
    responseStream: false,
    requestType: querier_pb.ProfileTypesRequest,
    responseType: querier_pb.ProfileTypesResponse,
    requestSerialize: serialize_querier_v1_ProfileTypesRequest,
    requestDeserialize: deserialize_querier_v1_ProfileTypesRequest,
    responseSerialize: serialize_querier_v1_ProfileTypesResponse,
    responseDeserialize: deserialize_querier_v1_ProfileTypesResponse,
  },
  // LabelValues returns the existing label values for the provided label names.
labelValues: {
    path: '/querier.v1.QuerierService/LabelValues',
    requestStream: false,
    responseStream: false,
    requestType: types_v1_types_pb.LabelValuesRequest,
    responseType: types_v1_types_pb.LabelValuesResponse,
    requestSerialize: serialize_types_v1_LabelValuesRequest,
    requestDeserialize: deserialize_types_v1_LabelValuesRequest,
    responseSerialize: serialize_types_v1_LabelValuesResponse,
    responseDeserialize: deserialize_types_v1_LabelValuesResponse,
  },
  // LabelNames returns a list of the existing label names.
labelNames: {
    path: '/querier.v1.QuerierService/LabelNames',
    requestStream: false,
    responseStream: false,
    requestType: types_v1_types_pb.LabelNamesRequest,
    responseType: types_v1_types_pb.LabelNamesResponse,
    requestSerialize: serialize_types_v1_LabelNamesRequest,
    requestDeserialize: deserialize_types_v1_LabelNamesRequest,
    responseSerialize: serialize_types_v1_LabelNamesResponse,
    responseDeserialize: deserialize_types_v1_LabelNamesResponse,
  },
  // Series returns profiles series matching the request. A series is a unique label set.
series: {
    path: '/querier.v1.QuerierService/Series',
    requestStream: false,
    responseStream: false,
    requestType: querier_pb.SeriesRequest,
    responseType: querier_pb.SeriesResponse,
    requestSerialize: serialize_querier_v1_SeriesRequest,
    requestDeserialize: deserialize_querier_v1_SeriesRequest,
    responseSerialize: serialize_querier_v1_SeriesResponse,
    responseDeserialize: deserialize_querier_v1_SeriesResponse,
  },
  // SelectMergeStacktraces returns matching profiles aggregated in a flamegraph format. It will combine samples from within the same callstack, with each element being grouped by its function name.
selectMergeStacktraces: {
    path: '/querier.v1.QuerierService/SelectMergeStacktraces',
    requestStream: false,
    responseStream: false,
    requestType: querier_pb.SelectMergeStacktracesRequest,
    responseType: querier_pb.SelectMergeStacktracesResponse,
    requestSerialize: serialize_querier_v1_SelectMergeStacktracesRequest,
    requestDeserialize: deserialize_querier_v1_SelectMergeStacktracesRequest,
    responseSerialize: serialize_querier_v1_SelectMergeStacktracesResponse,
    responseDeserialize: deserialize_querier_v1_SelectMergeStacktracesResponse,
  },
  // SelectMergeSpanProfile returns matching profiles aggregated in a flamegraph format. It will combine samples from within the same callstack, with each element being grouped by its function name.
selectMergeSpanProfile: {
    path: '/querier.v1.QuerierService/SelectMergeSpanProfile',
    requestStream: false,
    responseStream: false,
    requestType: querier_pb.SelectMergeSpanProfileRequest,
    responseType: querier_pb.SelectMergeSpanProfileResponse,
    requestSerialize: serialize_querier_v1_SelectMergeSpanProfileRequest,
    requestDeserialize: deserialize_querier_v1_SelectMergeSpanProfileRequest,
    responseSerialize: serialize_querier_v1_SelectMergeSpanProfileResponse,
    responseDeserialize: deserialize_querier_v1_SelectMergeSpanProfileResponse,
  },
  // SelectMergeProfile returns matching profiles aggregated in pprof format. It will contain all information stored (so including filenames and line number, if ingested).
selectMergeProfile: {
    path: '/querier.v1.QuerierService/SelectMergeProfile',
    requestStream: false,
    responseStream: false,
    requestType: querier_pb.SelectMergeProfileRequest,
    responseType: google_v1_profile_pb.Profile,
    requestSerialize: serialize_querier_v1_SelectMergeProfileRequest,
    requestDeserialize: deserialize_querier_v1_SelectMergeProfileRequest,
    responseSerialize: serialize_google_v1_Profile,
    responseDeserialize: deserialize_google_v1_Profile,
  },
  // SelectSeries returns a time series for the total sum of the requested profiles.
selectSeries: {
    path: '/querier.v1.QuerierService/SelectSeries',
    requestStream: false,
    responseStream: false,
    requestType: querier_pb.SelectSeriesRequest,
    responseType: querier_pb.SelectSeriesResponse,
    requestSerialize: serialize_querier_v1_SelectSeriesRequest,
    requestDeserialize: deserialize_querier_v1_SelectSeriesRequest,
    responseSerialize: serialize_querier_v1_SelectSeriesResponse,
    responseDeserialize: deserialize_querier_v1_SelectSeriesResponse,
  },
  // Diff returns a diff of two profiles
diff: {
    path: '/querier.v1.QuerierService/Diff',
    requestStream: false,
    responseStream: false,
    requestType: querier_pb.DiffRequest,
    responseType: querier_pb.DiffResponse,
    requestSerialize: serialize_querier_v1_DiffRequest,
    requestDeserialize: deserialize_querier_v1_DiffRequest,
    responseSerialize: serialize_querier_v1_DiffResponse,
    responseDeserialize: deserialize_querier_v1_DiffResponse,
  },
  // GetProfileStats returns profile stats for the current tenant.
getProfileStats: {
    path: '/querier.v1.QuerierService/GetProfileStats',
    requestStream: false,
    responseStream: false,
    requestType: types_v1_types_pb.GetProfileStatsRequest,
    responseType: types_v1_types_pb.GetProfileStatsResponse,
    requestSerialize: serialize_types_v1_GetProfileStatsRequest,
    requestDeserialize: deserialize_types_v1_GetProfileStatsRequest,
    responseSerialize: serialize_types_v1_GetProfileStatsResponse,
    responseDeserialize: deserialize_types_v1_GetProfileStatsResponse,
  },
  analyzeQuery: {
    path: '/querier.v1.QuerierService/AnalyzeQuery',
    requestStream: false,
    responseStream: false,
    requestType: querier_pb.AnalyzeQueryRequest,
    responseType: querier_pb.AnalyzeQueryResponse,
    requestSerialize: serialize_querier_v1_AnalyzeQueryRequest,
    requestDeserialize: deserialize_querier_v1_AnalyzeQueryRequest,
    responseSerialize: serialize_querier_v1_AnalyzeQueryResponse,
    responseDeserialize: deserialize_querier_v1_AnalyzeQueryResponse,
  },
};

exports.QuerierServiceClient = grpc.makeGenericClientConstructor(QuerierServiceService);
