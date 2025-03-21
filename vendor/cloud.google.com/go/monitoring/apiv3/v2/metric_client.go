// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go_gapic. DO NOT EDIT.

package monitoring

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

var newMetricClientHook clientHook

// MetricCallOptions contains the retry settings for each method of MetricClient.
type MetricCallOptions struct {
	ListMonitoredResourceDescriptors []gax.CallOption
	GetMonitoredResourceDescriptor   []gax.CallOption
	ListMetricDescriptors            []gax.CallOption
	GetMetricDescriptor              []gax.CallOption
	CreateMetricDescriptor           []gax.CallOption
	DeleteMetricDescriptor           []gax.CallOption
	ListTimeSeries                   []gax.CallOption
	CreateTimeSeries                 []gax.CallOption
	CreateServiceTimeSeries          []gax.CallOption
}

func defaultMetricGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("monitoring.googleapis.com:443"),
		internaloption.WithDefaultEndpointTemplate("monitoring.UNIVERSE_DOMAIN:443"),
		internaloption.WithDefaultMTLSEndpoint("monitoring.mtls.googleapis.com:443"),
		internaloption.WithDefaultUniverseDomain("googleapis.com"),
		internaloption.WithDefaultAudience("https://monitoring.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		internaloption.EnableJwtWithScope(),
		internaloption.EnableNewAuthLibrary(),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultMetricCallOptions() *MetricCallOptions {
	return &MetricCallOptions{
		ListMonitoredResourceDescriptors: []gax.CallOption{
			gax.WithTimeout(30000 * time.Millisecond),
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        30000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetMonitoredResourceDescriptor: []gax.CallOption{
			gax.WithTimeout(30000 * time.Millisecond),
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        30000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListMetricDescriptors: []gax.CallOption{
			gax.WithTimeout(30000 * time.Millisecond),
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        30000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetMetricDescriptor: []gax.CallOption{
			gax.WithTimeout(30000 * time.Millisecond),
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        30000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		CreateMetricDescriptor: []gax.CallOption{
			gax.WithTimeout(12000 * time.Millisecond),
		},
		DeleteMetricDescriptor: []gax.CallOption{
			gax.WithTimeout(30000 * time.Millisecond),
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        30000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListTimeSeries: []gax.CallOption{
			gax.WithTimeout(90000 * time.Millisecond),
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        30000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		CreateTimeSeries: []gax.CallOption{
			gax.WithTimeout(12000 * time.Millisecond),
		},
		CreateServiceTimeSeries: []gax.CallOption{},
	}
}

// internalMetricClient is an interface that defines the methods available from Cloud Monitoring API.
type internalMetricClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	ListMonitoredResourceDescriptors(context.Context, *monitoringpb.ListMonitoredResourceDescriptorsRequest, ...gax.CallOption) *MonitoredResourceDescriptorIterator
	GetMonitoredResourceDescriptor(context.Context, *monitoringpb.GetMonitoredResourceDescriptorRequest, ...gax.CallOption) (*monitoredrespb.MonitoredResourceDescriptor, error)
	ListMetricDescriptors(context.Context, *monitoringpb.ListMetricDescriptorsRequest, ...gax.CallOption) *MetricDescriptorIterator
	GetMetricDescriptor(context.Context, *monitoringpb.GetMetricDescriptorRequest, ...gax.CallOption) (*metricpb.MetricDescriptor, error)
	CreateMetricDescriptor(context.Context, *monitoringpb.CreateMetricDescriptorRequest, ...gax.CallOption) (*metricpb.MetricDescriptor, error)
	DeleteMetricDescriptor(context.Context, *monitoringpb.DeleteMetricDescriptorRequest, ...gax.CallOption) error
	ListTimeSeries(context.Context, *monitoringpb.ListTimeSeriesRequest, ...gax.CallOption) *TimeSeriesIterator
	CreateTimeSeries(context.Context, *monitoringpb.CreateTimeSeriesRequest, ...gax.CallOption) error
	CreateServiceTimeSeries(context.Context, *monitoringpb.CreateTimeSeriesRequest, ...gax.CallOption) error
}

// MetricClient is a client for interacting with Cloud Monitoring API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// Manages metric descriptors, monitored resource descriptors, and
// time series data.
type MetricClient struct {
	// The internal transport-dependent client.
	internalClient internalMetricClient

	// The call options for this service.
	CallOptions *MetricCallOptions
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *MetricClient) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *MetricClient) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated: Connections are now pooled so this method does not always
// return the same resource.
func (c *MetricClient) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// ListMonitoredResourceDescriptors lists monitored resource descriptors that match a filter.
func (c *MetricClient) ListMonitoredResourceDescriptors(ctx context.Context, req *monitoringpb.ListMonitoredResourceDescriptorsRequest, opts ...gax.CallOption) *MonitoredResourceDescriptorIterator {
	return c.internalClient.ListMonitoredResourceDescriptors(ctx, req, opts...)
}

// GetMonitoredResourceDescriptor gets a single monitored resource descriptor.
func (c *MetricClient) GetMonitoredResourceDescriptor(ctx context.Context, req *monitoringpb.GetMonitoredResourceDescriptorRequest, opts ...gax.CallOption) (*monitoredrespb.MonitoredResourceDescriptor, error) {
	return c.internalClient.GetMonitoredResourceDescriptor(ctx, req, opts...)
}

// ListMetricDescriptors lists metric descriptors that match a filter.
func (c *MetricClient) ListMetricDescriptors(ctx context.Context, req *monitoringpb.ListMetricDescriptorsRequest, opts ...gax.CallOption) *MetricDescriptorIterator {
	return c.internalClient.ListMetricDescriptors(ctx, req, opts...)
}

// GetMetricDescriptor gets a single metric descriptor.
func (c *MetricClient) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest, opts ...gax.CallOption) (*metricpb.MetricDescriptor, error) {
	return c.internalClient.GetMetricDescriptor(ctx, req, opts...)
}

// CreateMetricDescriptor creates a new metric descriptor.
// The creation is executed asynchronously.
// User-created metric descriptors define
// custom metrics (at https://cloud.google.com/monitoring/custom-metrics).
// The metric descriptor is updated if it already exists,
// except that metric labels are never removed.
func (c *MetricClient) CreateMetricDescriptor(ctx context.Context, req *monitoringpb.CreateMetricDescriptorRequest, opts ...gax.CallOption) (*metricpb.MetricDescriptor, error) {
	return c.internalClient.CreateMetricDescriptor(ctx, req, opts...)
}

// DeleteMetricDescriptor deletes a metric descriptor. Only user-created
// custom metrics (at https://cloud.google.com/monitoring/custom-metrics) can be
// deleted.
func (c *MetricClient) DeleteMetricDescriptor(ctx context.Context, req *monitoringpb.DeleteMetricDescriptorRequest, opts ...gax.CallOption) error {
	return c.internalClient.DeleteMetricDescriptor(ctx, req, opts...)
}

// ListTimeSeries lists time series that match a filter.
func (c *MetricClient) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest, opts ...gax.CallOption) *TimeSeriesIterator {
	return c.internalClient.ListTimeSeries(ctx, req, opts...)
}

// CreateTimeSeries creates or adds data to one or more time series.
// The response is empty if all time series in the request were written.
// If any time series could not be written, a corresponding failure message is
// included in the error response.
// This method does not support
// resource locations constraint of an organization
// policy (at https://cloud.google.com/resource-manager/docs/organization-policy/defining-locations#setting_the_organization_policy).
func (c *MetricClient) CreateTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest, opts ...gax.CallOption) error {
	return c.internalClient.CreateTimeSeries(ctx, req, opts...)
}

// CreateServiceTimeSeries creates or adds data to one or more service time series. A service time
// series is a time series for a metric from a Google Cloud service. The
// response is empty if all time series in the request were written. If any
// time series could not be written, a corresponding failure message is
// included in the error response. This endpoint rejects writes to
// user-defined metrics.
// This method is only for use by Google Cloud services. Use
// projects.timeSeries.create
// instead.
func (c *MetricClient) CreateServiceTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest, opts ...gax.CallOption) error {
	return c.internalClient.CreateServiceTimeSeries(ctx, req, opts...)
}

// metricGRPCClient is a client for interacting with Cloud Monitoring API over gRPC transport.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type metricGRPCClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// Points back to the CallOptions field of the containing MetricClient
	CallOptions **MetricCallOptions

	// The gRPC API client.
	metricClient monitoringpb.MetricServiceClient

	// The x-goog-* metadata to be sent with each request.
	xGoogHeaders []string

	logger *slog.Logger
}

// NewMetricClient creates a new metric service client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// Manages metric descriptors, monitored resource descriptors, and
// time series data.
func NewMetricClient(ctx context.Context, opts ...option.ClientOption) (*MetricClient, error) {
	clientOpts := defaultMetricGRPCClientOptions()
	if newMetricClientHook != nil {
		hookOpts, err := newMetricClientHook(ctx, clientHookParams{})
		if err != nil {
			return nil, err
		}
		clientOpts = append(clientOpts, hookOpts...)
	}

	connPool, err := gtransport.DialPool(ctx, append(clientOpts, opts...)...)
	if err != nil {
		return nil, err
	}
	client := MetricClient{CallOptions: defaultMetricCallOptions()}

	c := &metricGRPCClient{
		connPool:     connPool,
		metricClient: monitoringpb.NewMetricServiceClient(connPool),
		CallOptions:  &client.CallOptions,
		logger:       internaloption.GetLogger(opts),
	}
	c.setGoogleClientInfo()

	client.internalClient = c

	return &client, nil
}

// Connection returns a connection to the API service.
//
// Deprecated: Connections are now pooled so this method does not always
// return the same resource.
func (c *metricGRPCClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *metricGRPCClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", gax.GoVersion}, keyval...)
	kv = append(kv, "gapic", getVersionClient(), "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogHeaders = []string{
		"x-goog-api-client", gax.XGoogHeader(kv...),
	}
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *metricGRPCClient) Close() error {
	return c.connPool.Close()
}

func (c *metricGRPCClient) ListMonitoredResourceDescriptors(ctx context.Context, req *monitoringpb.ListMonitoredResourceDescriptorsRequest, opts ...gax.CallOption) *MonitoredResourceDescriptorIterator {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).ListMonitoredResourceDescriptors[0:len((*c.CallOptions).ListMonitoredResourceDescriptors):len((*c.CallOptions).ListMonitoredResourceDescriptors)], opts...)
	it := &MonitoredResourceDescriptorIterator{}
	req = proto.Clone(req).(*monitoringpb.ListMonitoredResourceDescriptorsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*monitoredrespb.MonitoredResourceDescriptor, string, error) {
		resp := &monitoringpb.ListMonitoredResourceDescriptorsResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = executeRPC(ctx, c.metricClient.ListMonitoredResourceDescriptors, req, settings.GRPC, c.logger, "ListMonitoredResourceDescriptors")
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetResourceDescriptors(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}

func (c *metricGRPCClient) GetMonitoredResourceDescriptor(ctx context.Context, req *monitoringpb.GetMonitoredResourceDescriptorRequest, opts ...gax.CallOption) (*monitoredrespb.MonitoredResourceDescriptor, error) {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).GetMonitoredResourceDescriptor[0:len((*c.CallOptions).GetMonitoredResourceDescriptor):len((*c.CallOptions).GetMonitoredResourceDescriptor)], opts...)
	var resp *monitoredrespb.MonitoredResourceDescriptor
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = executeRPC(ctx, c.metricClient.GetMonitoredResourceDescriptor, req, settings.GRPC, c.logger, "GetMonitoredResourceDescriptor")
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *metricGRPCClient) ListMetricDescriptors(ctx context.Context, req *monitoringpb.ListMetricDescriptorsRequest, opts ...gax.CallOption) *MetricDescriptorIterator {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).ListMetricDescriptors[0:len((*c.CallOptions).ListMetricDescriptors):len((*c.CallOptions).ListMetricDescriptors)], opts...)
	it := &MetricDescriptorIterator{}
	req = proto.Clone(req).(*monitoringpb.ListMetricDescriptorsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*metricpb.MetricDescriptor, string, error) {
		resp := &monitoringpb.ListMetricDescriptorsResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = executeRPC(ctx, c.metricClient.ListMetricDescriptors, req, settings.GRPC, c.logger, "ListMetricDescriptors")
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetMetricDescriptors(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}

func (c *metricGRPCClient) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest, opts ...gax.CallOption) (*metricpb.MetricDescriptor, error) {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).GetMetricDescriptor[0:len((*c.CallOptions).GetMetricDescriptor):len((*c.CallOptions).GetMetricDescriptor)], opts...)
	var resp *metricpb.MetricDescriptor
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = executeRPC(ctx, c.metricClient.GetMetricDescriptor, req, settings.GRPC, c.logger, "GetMetricDescriptor")
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *metricGRPCClient) CreateMetricDescriptor(ctx context.Context, req *monitoringpb.CreateMetricDescriptorRequest, opts ...gax.CallOption) (*metricpb.MetricDescriptor, error) {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).CreateMetricDescriptor[0:len((*c.CallOptions).CreateMetricDescriptor):len((*c.CallOptions).CreateMetricDescriptor)], opts...)
	var resp *metricpb.MetricDescriptor
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = executeRPC(ctx, c.metricClient.CreateMetricDescriptor, req, settings.GRPC, c.logger, "CreateMetricDescriptor")
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *metricGRPCClient) DeleteMetricDescriptor(ctx context.Context, req *monitoringpb.DeleteMetricDescriptorRequest, opts ...gax.CallOption) error {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).DeleteMetricDescriptor[0:len((*c.CallOptions).DeleteMetricDescriptor):len((*c.CallOptions).DeleteMetricDescriptor)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = executeRPC(ctx, c.metricClient.DeleteMetricDescriptor, req, settings.GRPC, c.logger, "DeleteMetricDescriptor")
		return err
	}, opts...)
	return err
}

func (c *metricGRPCClient) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest, opts ...gax.CallOption) *TimeSeriesIterator {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).ListTimeSeries[0:len((*c.CallOptions).ListTimeSeries):len((*c.CallOptions).ListTimeSeries)], opts...)
	it := &TimeSeriesIterator{}
	req = proto.Clone(req).(*monitoringpb.ListTimeSeriesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*monitoringpb.TimeSeries, string, error) {
		resp := &monitoringpb.ListTimeSeriesResponse{}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else if pageSize != 0 {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = executeRPC(ctx, c.metricClient.ListTimeSeries, req, settings.GRPC, c.logger, "ListTimeSeries")
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetTimeSeries(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()

	return it
}

func (c *metricGRPCClient) CreateTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest, opts ...gax.CallOption) error {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).CreateTimeSeries[0:len((*c.CallOptions).CreateTimeSeries):len((*c.CallOptions).CreateTimeSeries)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = executeRPC(ctx, c.metricClient.CreateTimeSeries, req, settings.GRPC, c.logger, "CreateTimeSeries")
		return err
	}, opts...)
	return err
}

func (c *metricGRPCClient) CreateServiceTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest, opts ...gax.CallOption) error {
	hds := []string{"x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName()))}

	hds = append(c.xGoogHeaders, hds...)
	ctx = gax.InsertMetadataIntoOutgoingContext(ctx, hds...)
	opts = append((*c.CallOptions).CreateServiceTimeSeries[0:len((*c.CallOptions).CreateServiceTimeSeries):len((*c.CallOptions).CreateServiceTimeSeries)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = executeRPC(ctx, c.metricClient.CreateServiceTimeSeries, req, settings.GRPC, c.logger, "CreateServiceTimeSeries")
		return err
	}, opts...)
	return err
}
