// Code generated by protoc-gen-go.
// source: vehiclelocation.proto
// DO NOT EDIT!

/*
Package capmetricsd is a generated protocol buffer package.

It is generated from these files:
    vehiclelocation.proto

It has these top-level messages:
    VehicleLocation
*/
package agency

import proto "github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/golang/protobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type VehicleLocation struct {
	VehicleId        *string  `protobuf:"bytes,1,opt,name=vehicle_id" json:"vehicle_id,omitempty"`
	Timestamp        *int64   `protobuf:"varint,2,opt,name=timestamp" json:"timestamp,omitempty"`
	Speed            *float32 `protobuf:"fixed32,3,opt,name=speed" json:"speed,omitempty"`
	RouteId          *string  `protobuf:"bytes,4,opt,name=route_id" json:"route_id,omitempty"`
	TripId           *string  `protobuf:"bytes,5,opt,name=trip_id" json:"trip_id,omitempty"`
	Bearing          *float32 `protobuf:"fixed32,6,opt,name=bearing" json:"bearing,omitempty"`
	Latitude         *float32 `protobuf:"fixed32,7,opt,name=latitude" json:"latitude,omitempty"`
	Longitude        *float32 `protobuf:"fixed32,8,opt,name=longitude" json:"longitude,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *VehicleLocation) Reset()         { *m = VehicleLocation{} }
func (m *VehicleLocation) String() string { return proto.CompactTextString(m) }
func (*VehicleLocation) ProtoMessage()    {}

func (m *VehicleLocation) GetVehicleId() string {
	if m != nil && m.VehicleId != nil {
		return *m.VehicleId
	}
	return ""
}

func (m *VehicleLocation) GetTimestamp() int64 {
	if m != nil && m.Timestamp != nil {
		return *m.Timestamp
	}
	return 0
}

func (m *VehicleLocation) GetSpeed() float32 {
	if m != nil && m.Speed != nil {
		return *m.Speed
	}
	return 0
}

func (m *VehicleLocation) GetRouteId() string {
	if m != nil && m.RouteId != nil {
		return *m.RouteId
	}
	return ""
}

func (m *VehicleLocation) GetTripId() string {
	if m != nil && m.TripId != nil {
		return *m.TripId
	}
	return ""
}

func (m *VehicleLocation) GetBearing() float32 {
	if m != nil && m.Bearing != nil {
		return *m.Bearing
	}
	return 0
}

func (m *VehicleLocation) GetLatitude() float32 {
	if m != nil && m.Latitude != nil {
		return *m.Latitude
	}
	return 0
}

func (m *VehicleLocation) GetLongitude() float32 {
	if m != nil && m.Longitude != nil {
		return *m.Longitude
	}
	return 0
}
