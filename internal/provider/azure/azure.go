package azure

import (
	"context"
	"errors"
	"fmt"
	"strings"

	dns "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/dns/armdns"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/trafficmanager/armtrafficmanager"
	externaldnsendpoint "sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/plan"

	"github.com/go-logr/logr"
	"gopkg.in/yaml.v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kuadrant/dns-operator/api/v1alpha1"
	externaldnsproviderazure "github.com/kuadrant/dns-operator/internal/external-dns/provider/azure"
	"github.com/kuadrant/dns-operator/internal/provider"
)

type AzureProvider struct {
	*externaldnsproviderazure.AzureProvider
	azureConfig externaldnsproviderazure.Config
	logger      logr.Logger
}

var _ provider.Provider = &AzureProvider{}

func NewAzureProviderFromSecret(ctx context.Context, s *v1.Secret, c provider.Config) (provider.Provider, error) {
	if string(s.Data["azure.json"]) == "" {
		return nil, fmt.Errorf("the Azure provider credentials is empty")
	}

	configString := string(s.Data["azure.json"])
	var azureConfig externaldnsproviderazure.Config
	err := yaml.Unmarshal([]byte(configString), &azureConfig)
	if err != nil {
		return nil, err
	}

	logger := crlog.FromContext(ctx).
		WithName("azure-dns").
		WithValues("tenantId", azureConfig.TenantID, "resourceGroup", azureConfig.ResourceGroup)
	ctx = crlog.IntoContext(ctx, logger)

	azureConfig.DomainFilter = c.DomainFilter
	azureConfig.ZoneNameFilter = c.DomainFilter
	azureConfig.IDFilter = c.ZoneIDFilter
	azureConfig.DryRun = false

	azureProvider, err := externaldnsproviderazure.NewAzureProviderFromConfig(ctx, azureConfig)

	if err != nil {
		return nil, fmt.Errorf("unable to create azure provider: %s", err)
	}

	p := &AzureProvider{
		AzureProvider: azureProvider,
		azureConfig:   azureConfig,
		logger:        logger,
	}

	return p, nil

}

// Register this Provider with the provider factory
func init() {
	provider.RegisterProvider("azure", NewAzureProviderFromSecret, false)
}

func (p *AzureProvider) HealthCheckReconciler() provider.HealthCheckReconciler {
	return NewAzureHealthCheckReconciler()
}

func (p *AzureProvider) ProviderSpecific() provider.ProviderSpecificLabels {
	return provider.ProviderSpecificLabels{}
}

func (p *AzureProvider) EnsureManagedZone(ctx context.Context, managedZone *v1alpha1.ManagedZone) (provider.ManagedZoneOutput, error) {
	var zoneID string

	if managedZone.Spec.ID != "" {
		zoneID = managedZone.Spec.ID
	} else {
		zoneID = managedZone.Status.ID
	}

	if zoneID != "" {
		//Get existing managed zone
		return p.getManagedZone(ctx, zoneID)
	}
	//Create new managed zone
	return p.createManagedZone(ctx, managedZone)
}

// DeleteManagedZone not implemented as managed zones are going away
func (p *AzureProvider) DeleteManagedZone(_ *v1alpha1.ManagedZone) error {
	return nil // p.zonesClient.Delete(p.project, managedZone.Status.ID).Do()
}

func (p *AzureProvider) getManagedZone(ctx context.Context, zoneID string) (provider.ManagedZoneOutput, error) {
	logger := crlog.FromContext(ctx).WithName("getManagedZone")
	zones, err := p.Zones(ctx)
	if err != nil {
		return provider.ManagedZoneOutput{}, err
	}

	for _, zone := range zones {
		logger.Info("comparing zone IDs", "found zone ID", zone.ID, "wanted zone ID", zoneID)
		if *zone.ID == zoneID {
			logger.Info("found zone ID", "found zone ID", zoneID, "wanted zone ID", zoneID)
			return provider.ManagedZoneOutput{
				ID:          *zone.ID,
				DNSName:     *zone.Name,
				NameServers: zone.Properties.NameServers,
				RecordCount: *zone.Properties.NumberOfRecordSets,
			}, nil
		}
	}

	return provider.ManagedZoneOutput{}, fmt.Errorf("zone %s not found", zoneID)
}

// createManagedZone not implemented as managed zones are going away
func (p *AzureProvider) createManagedZone(_ context.Context, _ *v1alpha1.ManagedZone) (provider.ManagedZoneOutput, error) {
	return provider.ManagedZoneOutput{}, nil
}

// Records gets the current records.
//
// Returns the current records or an error if the operation failed.
func (p *AzureProvider) Records(ctx context.Context) (endpoints []*externaldnsendpoint.Endpoint, _ error) {
	zones, err := p.Zones(ctx)
	if err != nil {
		return nil, err
	}

	p.logger.Info("getting records from azure")
	for _, zone := range zones {
		testForTrafficManagerProfile := []dns.RecordSet{}
		pager := p.RecordSetsClient.NewListAllByDNSZonePager(p.ResourceGroup, *zone.Name, &dns.RecordSetsClientListAllByDNSZoneOptions{Top: nil})
		for pager.More() {
			nextResult, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, recordSet := range nextResult.Value {
				if recordSet.Name == nil || recordSet.Type == nil {
					p.logger.Error(errors.New("record set has nil name or type"), "Skipping invalid record set with nil name or type")
					continue
				}
				recordType := strings.TrimPrefix(*recordSet.Type, "Microsoft.Network/dnszones/")
				if !p.SupportedRecordType(recordType) {
					continue
				}
				name := externaldnsproviderazure.FormatAzureDNSName(*recordSet.Name, *zone.Name)
				if len(p.ZoneNameFilter.Filters) > 0 && !p.DomainFilter.Match(name) {
					p.logger.V(1).Info("skipping return of record because it was filtered out by the specified --domain-filter", "record name", name)
					continue
				}
				targets := externaldnsproviderazure.ExtractAzureTargets(recordSet)
				if len(targets) == 0 {
					testForTrafficManagerProfile = append(testForTrafficManagerProfile, *recordSet)
					p.logger.V(1).Info("failed to extract targets from record set", "record name", name, "record type", recordType)
					continue
				}
				var ttl externaldnsendpoint.TTL
				if recordSet.Properties.TTL != nil {
					ttl = externaldnsendpoint.TTL(*recordSet.Properties.TTL)
				}
				ep := externaldnsendpoint.NewEndpointWithTTL(name, recordType, ttl, targets...)

				p.logger.V(1).Info("found record set", "record type", ep.RecordType, "DNS Name", ep.DNSName, "targets", ep.Targets)
				endpoints = append(endpoints, ep)
			}
		}
		for _, recordSet := range testForTrafficManagerProfile {
			tmEndpoints, err := p.endpointsFromTrafficManagers(ctx, &recordSet)
			if err != nil {
				p.logger.Error(err, "error extracting traffic manager profile for recordset", "recordset", recordSet)
			}
			endpoints = append(endpoints, tmEndpoints...)
		}
	}

	return endpoints, nil
}

func (p *AzureProvider) endpointsFromTrafficManagers(ctx context.Context, recordSet *dns.RecordSet) (endpoints []*externaldnsendpoint.Endpoint, err error) {
	p.logger.Info("getting endpoints from record set", "record set", recordSet)
	if recordSet.Properties.TargetResource == nil || recordSet.Properties.TargetResource.ID == nil {
		return nil, nil
	}
	profileNameParts := strings.Split(*recordSet.Properties.TargetResource.ID, "/")
	profileName := profileNameParts[len(profileNameParts)-1]
	profile, err := p.TrafficManagerProfilesClient.Get(ctx, p.ResourceGroup, profileName, nil)
	if err != nil {
		return nil, err
	}

	p.logger.Info("get profile for recordset", "record set", recordSet, "profile", profile)

	recordType := strings.Split(*recordSet.Type, "/")

	ep := externaldnsendpoint.Endpoint{}
	ep.DNSName = *recordSet.Properties.Fqdn
	ep.WithProviderSpecific("routingpolicy", string(ptr.Deref(profile.Properties.TrafficRoutingMethod, "")))

	ep.RecordTTL = externaldnsendpoint.TTL(*recordSet.Properties.TTL)
	ep.RecordType = recordType[len(recordType)-1]

	for _, e := range profile.Properties.Endpoints {
		ep.Targets = append(ep.Targets, ptr.Deref(e.Properties.Target, ""))
		ep.WithProviderSpecific(*e.Properties.Target, *e.Properties.GeoMapping[0])
	}
	endpoints = append(endpoints, &ep)
	p.logger.V(1).Info("built endpoint", "endpoint", ep)
	return endpoints, nil
}

// AdjustEndpoints takes source endpoints and translates them to an azure specific format
func (p *AzureProvider) AdjustEndpoints(endpoints []*externaldnsendpoint.Endpoint) ([]*externaldnsendpoint.Endpoint, error) {
	p.logger.Info("adjusting azure endpoints")
	return endpointsToAzureFormat(endpoints), nil
}

// ApplyChanges applies the given changes.
//
// Returns nil if the operation was successful or an error if the operation failed.
func (p *AzureProvider) ApplyChanges(ctx context.Context, changes *plan.Changes) error {
	zones, err := p.Zones(ctx)
	if err != nil {
		return err
	}

	deleted, updated := p.MapChanges(zones, changes)

	p.logger.Info("applying changes", "deleted", len(deleted), "updated", len(updated))
	p.DeleteRecords(ctx, deleted)
	p.UpdateRecords(ctx, updated)
	return nil
}

func (p *AzureProvider) DeleteRecords(ctx context.Context, deleted externaldnsproviderazure.AzureChangeMap) {
	p.logger.Info("deleting records")
	// Delete records first
	for zone, endpoints := range deleted {
		for _, ep := range endpoints {
			if _, ok := ep.GetProviderSpecificProperty("routingpolicy"); ok && ep.RecordType != "TXT" {
				p.logger.Info("deleting endpoint with routingpolicy", "endpoint", ep)
			} else {
				name := p.RecordSetNameForZone(zone, ep)
				if !p.DomainFilter.Match(ep.DNSName) {
					p.logger.V(1).Info("skipping deletion of record as it was filtered out by the specified --domain-filter", "record name", ep.DNSName)
					continue
				}
				if p.DryRun {
					p.logger.Info("would delete record", "record type", ep.RecordType, "record name", name, "zone", zone)
				} else {
					p.logger.Info("deleting record", "record type", ep.RecordType, "record name", name, "zone", zone)
					if _, err := p.RecordSetsClient.Delete(ctx, p.ResourceGroup, zone, name, dns.RecordType(ep.RecordType), nil); err != nil {
						p.logger.Error(err, "failed to delete record", "record type", ep.RecordType, "record name", name, "zone", zone)
					}
				}
			}
		}
	}
}

func (p *AzureProvider) UpdateRecords(ctx context.Context, updated externaldnsproviderazure.AzureChangeMap) {
	p.logger.Info("updating records")
	for zone, endpoints := range updated {
		for _, ep := range endpoints {
			if !p.DomainFilter.Match(ep.DNSName) {
				p.logger.V(1).Info("skipping update of record because it was filtered by the specified --domain-filter", "record name", ep.DNSName)
				continue
			}

			if _, ok := ep.GetProviderSpecificProperty("routingpolicy"); ok && ep.RecordType != "TXT" {
				profileName := p.ResourceGroup + "-" + strings.ReplaceAll(ep.DNSName, ".", "-")
				p.logger.Info("updating endpoint with routingpolicy", "endpoint", ep)
				tmEndpoints := []*armtrafficmanager.Endpoint{}
				for _, target := range ep.Targets {
					geo, ok := ep.GetProviderSpecificProperty(target)
					if !ok {
						p.logger.Error(fmt.Errorf("could not find geo string for target: '%s'", target), "no geo property set", "endpoint", ep)
						continue
					}
					tmEndpoint := armtrafficmanager.Endpoint{
						Type: ptr.To("Microsoft.Network/trafficManagerProfiles/externalEndpoints"),
						Name: ptr.To(strings.ReplaceAll(target, ".", "-")),
						Properties: &armtrafficmanager.EndpointProperties{
							GeoMapping:  []*string{ptr.To(geo)},
							Target:      ptr.To(target),
							AlwaysServe: ptr.To(armtrafficmanager.AlwaysServeEnabled),
						},
					}
					tmEndpoints = append(tmEndpoints, &tmEndpoint)
				}
				var ttl int64 = 60
				var port int64 = 80
				profile := armtrafficmanager.Profile{
					Location: ptr.To("global"),
					Properties: &armtrafficmanager.ProfileProperties{
						TrafficRoutingMethod: ptr.To(armtrafficmanager.TrafficRoutingMethodGeographic),
						Endpoints:            tmEndpoints,
						DNSConfig: &armtrafficmanager.DNSConfig{
							RelativeName: &profileName,
							TTL:          &ttl,
						},
						MonitorConfig: &armtrafficmanager.MonitorConfig{
							Path:     ptr.To("/"),
							Port:     &port,
							Protocol: ptr.To(armtrafficmanager.MonitorProtocolHTTP),
						},
					},
				}
				if p.DryRun {
					p.logger.Info("would update traffic manager profile", "name", profileName, "profile", profile)
					continue
				}
				p.logger.Info("updating traffic manager profile", "name", profileName, "profile", profile)
				tmResp, err := p.TrafficManagerProfilesClient.CreateOrUpdate(ctx, p.ResourceGroup, profileName, profile, nil)
				if err != nil {
					p.logger.Error(err, "error updating traffic manager", "name", profileName, "profile", profile)
				}

				name := p.RecordSetNameForZone(zone, ep)
				var epTTL int64 = int64(ep.RecordTTL)
				p.logger.Info("updating record to use traffic manager profile", "name", name, "profile ID", tmResp.ID)
				_, err = p.RecordSetsClient.CreateOrUpdate(
					ctx,
					p.ResourceGroup,
					zone,
					name,
					dns.RecordTypeCNAME,
					dns.RecordSet{
						Properties: &dns.RecordSetProperties{
							TTL: &epTTL,
							TargetResource: &dns.SubResource{
								ID: tmResp.ID,
							},
						},
					},
					nil,
				)

				if err != nil {
					p.logger.Error(err, "failed to update record", "record type", ep.RecordType, "record name", name, "zone", zone, "target", tmResp.ID)
				}
			} else {
				name := p.RecordSetNameForZone(zone, ep)
				if p.DryRun {
					p.logger.Info("would update record", "record type", ep.RecordType, "record name", name, "targets", ep.Targets, "zone", zone)
					continue
				}
				p.logger.Info("updating record", "record type", ep.RecordType, "record name", name, "targets", ep.Targets, "zone", zone)

				recordSet, err := p.NewRecordSet(ep)
				if err == nil {
					_, err = p.RecordSetsClient.CreateOrUpdate(
						ctx,
						p.ResourceGroup,
						zone,
						name,
						dns.RecordType(ep.RecordType),
						recordSet,
						nil,
					)
				}
				if err != nil {
					p.logger.Error(err, "failed to update record", "record type", ep.RecordType, "record name", name, "targets", ep.Targets, "zone", zone)
				}
			}
		}
	}
}

// endpointsToProviderFormat converts a list of endpoints into an azure specific format.
func endpointsToAzureFormat(eps []*externaldnsendpoint.Endpoint) []*externaldnsendpoint.Endpoint {
	endpointMap := make(map[string][]*externaldnsendpoint.Endpoint)
	for i := range eps {
		endpointMap[eps[i].DNSName] = append(endpointMap[eps[i].DNSName], eps[i])
	}

	var translatedEndpoints []*externaldnsendpoint.Endpoint

	for dnsName, endpoints := range endpointMap {
		// A set of endpoints belonging to the same group(`dnsName`) must always be of the same type, have the same ttl
		// and contain the same rrdata (weighted or geo), so we can just get that from the first endpoint in the list.
		ttl := int64(endpoints[0].RecordTTL)
		recordType := endpoints[0].RecordType
		_, isWeighted := endpoints[0].GetProviderSpecificProperty(v1alpha1.ProviderSpecificWeight)
		_, isGeo := endpoints[0].GetProviderSpecificProperty(v1alpha1.ProviderSpecificGeoCode)

		if !isGeo && !isWeighted {
			//ToDO DO we need to worry about there being more than one here?
			translatedEndpoints = append(translatedEndpoints, endpoints[0])
			continue
		}

		translatedEndpoint := externaldnsendpoint.NewEndpointWithTTL(dnsName, recordType, externaldnsendpoint.TTL(ttl))
		if isGeo {
			translatedEndpoint.WithProviderSpecific("routingpolicy", "Geographic")
		} else if isWeighted {
			translatedEndpoint.WithProviderSpecific("routingpolicy", "weighted")
		}

		//ToDo this has the potential to add duplicates
		for _, ep := range endpoints {
			for _, t := range ep.Targets {
				if isGeo {
					geo, _ := ep.GetProviderSpecificProperty(v1alpha1.ProviderSpecificGeoCode)
					if geo == "*" {
						continue
					}
					translatedEndpoint.WithProviderSpecific(t, geo)
				} else if isWeighted {
					weight, _ := ep.GetProviderSpecificProperty(v1alpha1.ProviderSpecificWeight)
					translatedEndpoint.WithProviderSpecific(t, weight)
				}
				translatedEndpoint.Targets = append(translatedEndpoint.Targets, t)
			}
		}

		translatedEndpoints = append(translatedEndpoints, translatedEndpoint)
	}
	return translatedEndpoints
}
