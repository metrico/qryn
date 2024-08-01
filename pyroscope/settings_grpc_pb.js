// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var settings_pb = require('./settings_pb.js');

function serialize_settings_v1_GetSettingsRequest(arg) {
  if (!(arg instanceof settings_pb.GetSettingsRequest)) {
    throw new Error('Expected argument of type settings.v1.GetSettingsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_settings_v1_GetSettingsRequest(buffer_arg) {
  return settings_pb.GetSettingsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_settings_v1_GetSettingsResponse(arg) {
  if (!(arg instanceof settings_pb.GetSettingsResponse)) {
    throw new Error('Expected argument of type settings.v1.GetSettingsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_settings_v1_GetSettingsResponse(buffer_arg) {
  return settings_pb.GetSettingsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_settings_v1_SetSettingsRequest(arg) {
  if (!(arg instanceof settings_pb.SetSettingsRequest)) {
    throw new Error('Expected argument of type settings.v1.SetSettingsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_settings_v1_SetSettingsRequest(buffer_arg) {
  return settings_pb.SetSettingsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_settings_v1_SetSettingsResponse(arg) {
  if (!(arg instanceof settings_pb.SetSettingsResponse)) {
    throw new Error('Expected argument of type settings.v1.SetSettingsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_settings_v1_SetSettingsResponse(buffer_arg) {
  return settings_pb.SetSettingsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


var SettingsServiceService = exports.SettingsServiceService = {
  get: {
    path: '/settings.v1.SettingsService/Get',
    requestStream: false,
    responseStream: false,
    requestType: settings_pb.GetSettingsRequest,
    responseType: settings_pb.GetSettingsResponse,
    requestSerialize: serialize_settings_v1_GetSettingsRequest,
    requestDeserialize: deserialize_settings_v1_GetSettingsRequest,
    responseSerialize: serialize_settings_v1_GetSettingsResponse,
    responseDeserialize: deserialize_settings_v1_GetSettingsResponse,
  },
  set: {
    path: '/settings.v1.SettingsService/Set',
    requestStream: false,
    responseStream: false,
    requestType: settings_pb.SetSettingsRequest,
    responseType: settings_pb.SetSettingsResponse,
    requestSerialize: serialize_settings_v1_SetSettingsRequest,
    requestDeserialize: deserialize_settings_v1_SetSettingsRequest,
    responseSerialize: serialize_settings_v1_SetSettingsResponse,
    responseDeserialize: deserialize_settings_v1_SetSettingsResponse,
  },
};

exports.SettingsServiceClient = grpc.makeGenericClientConstructor(SettingsServiceService);
