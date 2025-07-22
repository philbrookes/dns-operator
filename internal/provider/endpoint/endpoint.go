/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endpoint

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/kuadrant/dns-operator/api/v1alpha1"
	"github.com/kuadrant/dns-operator/internal/common"
	dnsOperatorProvider "github.com/kuadrant/dns-operator/internal/provider"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/plan"
	"sigs.k8s.io/external-dns/provider"
)

var (
	// ErrNoGVRSpecified when a provider secret has no defined GVR
	ErrNoGVRSpecified = errors.New("provider secret has no defined GVR")
	// ErrNoLabelSpecified when a provider secret has no defined zone label
	ErrNoLabelSpecified = errors.New("provider secret has no zone label specified")
)

var e provider.Provider = &EndpointProvider{}

type filter struct {
	domain string
}

// EndpointProvider - dns provider only used for testing purposes
// initialized as dns provider with no records
type EndpointProvider struct {
	provider.BaseProvider
	providerNS    string
	config        dnsOperatorProvider.Config
	object        client.Object
	logger        logr.Logger
	client        dynamic.Interface
	gvr           schema.GroupVersionResource
	labelSelector string
}

type EndpointZone struct {
	name     string
	rootHost string
	records  []*endpoint.Endpoint
}

// DNSZoneForHost return the first authoritative DNSRecord with the same DNSRecord.spec.rootHost
func (e *EndpointProvider) DNSZoneForHost(ctx context.Context, host string) (*dnsOperatorProvider.DNSZone, error) {
	zones, err := e.DNSZones(ctx)
	if err != nil {
		return nil, err
	}
	return dnsOperatorProvider.FindDNSZoneForHost(ctx, host, zones, false)
}

// ApplyChanges implements provider.Provider.
func (e *EndpointProvider) ApplyChanges(ctx context.Context, changes *plan.Changes) error {
	zoneAccessor, err := e.getZoneAccessorForZoneIDFilter(ctx)
	if err != nil {
		return err
	}

	for _, newEndpoint := range changes.Create {
		zoneAccessor.EnsureEndpoint(newEndpoint)
	}
	for _, updateEndpoint := range changes.UpdateNew {
		zoneAccessor.EnsureEndpoint(updateEndpoint)
	}
	for _, deleteEndpoint := range changes.Delete {
		zoneAccessor.RemoveEndpoint(deleteEndpoint)
	}
	_, err = e.client.Resource(e.gvr).Namespace(e.providerNS).Update(ctx, zoneAccessor.GetObject(), metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

// Records returns the list of endpoints
func (e *EndpointProvider) Records(ctx context.Context) ([]*endpoint.Endpoint, error) {
	zoneAccessor, err := e.getZoneAccessorForZoneIDFilter(ctx)
	if err != nil {
		return nil, err
	}
	// delabelledRecords := []*endpoint.Endpoint{}
	// for _, e := range zoneAccessor.GetEndpoints() {
	// 	delabelledEndpoint := e.DeepCopy()
	// 	delabelledEndpoint.Labels = map[string]string{}
	// 	delabelledRecords = append(delabelledRecords, delabelledEndpoint)
	// }
	// return delabelledRecords, nil
	return zoneAccessor.GetEndpoints(), nil
}

func (e *EndpointProvider) DNSZones(ctx context.Context) ([]dnsOperatorProvider.DNSZone, error) {
	var hzs []dnsOperatorProvider.DNSZone
	zones, err := e.client.Resource(e.gvr).Namespace(e.providerNS).List(
		ctx,
		metav1.ListOptions{LabelSelector: e.labelSelector},
	)
	if err != nil {
		return nil, err
	}

	for _, z := range zones.Items {
		za, err := NewEndpointAccessor(&z)
		if err != nil {
			e.logger.Info("badly formatted zone", "zone name", z.GetName())
			continue
		}
		hz := dnsOperatorProvider.DNSZone{
			ID:      z.GetName(),
			DNSName: za.GetRootHost(),
		}
		hzs = append(hzs, hz)
	}
	return hzs, nil
}

func (e *EndpointProvider) ProviderSpecific() dnsOperatorProvider.ProviderSpecificLabels {
	return dnsOperatorProvider.ProviderSpecificLabels{}
}

func (e *EndpointProvider) getZoneAccessorForZoneIDFilter(ctx context.Context) (*endpointAccessor, error) {
	if !e.config.ZoneIDFilter.IsConfigured() {
		return nil, fmt.Errorf("no zone id filter specified for Endpoint Provider")
	}

	e.logger.Info("getting zone accessor for zone id filter", "filter", e.config.ZoneIDFilter, "object", e.object)

	zone, err := e.client.Resource(e.gvr).Namespace(e.object.GetNamespace()).Get(ctx, e.config.ZoneIDFilter.ZoneIDs[0], metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return NewEndpointAccessor(zone)
}

// GetDomainFilter implements provider.Provider.
// Subtle: this method shadows the method (BaseProvider).GetDomainFilter of EndpointProvider.BaseProvider.
func (e *EndpointProvider) GetDomainFilter() endpoint.DomainFilter {
	return endpoint.DomainFilter{}
}

// Name implements provider.Provider.
func (e *EndpointProvider) Name() dnsOperatorProvider.DNSProviderName {
	return dnsOperatorProvider.DNSProviderEndpoint
}

// AdjustEndpoints nothing to do here
func (e *EndpointProvider) AdjustEndpoints(endpoints []*endpoint.Endpoint) ([]*endpoint.Endpoint, error) {
	return endpoints, nil
}

func NewProviderFromSecret(ctx context.Context, client dynamic.Interface, s *v1.Secret, providerConfig dnsOperatorProvider.Config) (dnsOperatorProvider.Provider, error) {
	logger := log.FromContext(ctx).WithName("endpoint-dns")
	ctx = log.IntoContext(ctx, logger)

	var gvr schema.GroupVersionResource
	var err error

	if gvrStr := string(s.Labels[v1alpha1.EndpointGVRKey]); gvrStr != "" {
		gvr, err = common.ParseGVRString(gvrStr)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrNoGVRSpecified
	}
	var labelSelector string
	if labelSelector = string(s.Labels[v1alpha1.EndpointLabelSelectorKey]); labelSelector == "" {
		return nil, ErrNoLabelSpecified
	}

	var labelValue string
	if labelValue = string(s.Labels[v1alpha1.EndpointLabelValueKey]); labelValue == "" {
		labelValue = "true"
	}

	providerNS := s.GetNamespace()

	endpointProvider := &EndpointProvider{
		logger:        logger,
		config:        providerConfig,
		client:        client,
		gvr:           gvr,
		labelSelector: labels.Set(metav1.LabelSelector{MatchLabels: map[string]string{labelSelector: labelValue}}.MatchLabels).String(),
		providerNS:    providerNS,
		object:        s,
	}

	return endpointProvider, nil
}

// Register this Provider with the provider factory
func init() {
	dnsOperatorProvider.RegisterProviderWithClient(dnsOperatorProvider.DNSProviderEndpoint.String(), NewProviderFromSecret, true)
}
