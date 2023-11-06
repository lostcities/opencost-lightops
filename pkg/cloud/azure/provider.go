package azure

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/commerce/armcommerce"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions"

	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util"
	"github.com/opencost/opencost/pkg/util/fileutil"
	"github.com/opencost/opencost/pkg/util/json"
	"github.com/opencost/opencost/pkg/util/timeutil"

	v1 "k8s.io/api/core/v1"
)

// if we get a better pricing option, set this to false.
var useBillingAccounPricing = true

const (
	AzureFilePremiumStorageClass        = "premium_smb"
	AzureFileStandardStorageClass       = "standard_smb"
	AzureDiskPremiumSSDStorageClass     = "Premium_LRS"
	AzureDiskPremiumSSDZRSStorageClass  = "Premium_ZRS"
	AzureDiskStandardSSDStorageClass    = "StandardSSD_LRS"
	AzureDiskStandardSSDZRSStorageClass = "StandardSSD_ZRS"
	AzureDiskStandardStorageClass       = "Standard_LRS"
	AzureDiskUltraSSDLRSStorageClass    = "UltraSSD_LRS"
	defaultSpotLabel                    = "kubernetes.azure.com/scalesetpriority"
	defaultSpotLabelValue               = "spot"
	AzureStorageUpdateType              = "AzureStorage"
)

var (
	regionCodeMappings = map[string]string{
		"ap": "asia",
		"au": "australia",
		"br": "brazil",
		"ca": "canada",
		"eu": "europe",
		"fr": "france",
		"in": "india",
		"ja": "japan",
		"kr": "korea",
		"uk": "uk",
		"us": "us",
		"za": "southafrica",
		"no": "norway",
		"ch": "switzerland",
		"de": "germany",
		"ue": "uae",
	}

	//mtBasic, _     = regexp.Compile("^BASIC.A\\d+[_Promo]*$")
	//mtStandardA, _ = regexp.Compile("^A\\d+[_Promo]*$")
	// mtStandardB, _ = regexp.Compile(`^Standard_B\d+m?[_v\d]*[_Promo]*$`)
	// mtStandardD, _ = regexp.Compile(`^Standard_D\d[_v\d]*[_Promo]*$`)
	// mtStandardE, _ = regexp.Compile(`^Standard_E\d+i?[_v\d]*[_Promo]*$`)
	// mtStandardF, _ = regexp.Compile(`^Standard_F\d+[_v\d]*[_Promo]*$`)
	// mtStandardG, _ = regexp.Compile(`^Standard_G\d+[_v\d]*[_Promo]*$`)
	// mtStandardL, _ = regexp.Compile(`^Standard_L\d+[_v\d]*[_Promo]*$`)
	// mtStandardM, _ = regexp.Compile(`^Standard_M\d+[m|t|l]*s[_v\d]*[_Promo]*$`)
	// mtStandardN, _ = regexp.Compile(`^Standard_N[C|D|V]\d+r?[_v\d]*[_Promo]*$`)

	// azure:///subscriptions/0badafdf-1234-abcd-wxyz-123456789/...
	//  => 0badafdf-1234-abcd-wxyz-123456789
	azureSubRegex = regexp.MustCompile("azure:///subscriptions/([^/]*)/*")
)

// List obtained by installing the Azure CLI tool "az", described here:
// https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-linux?pivots=apt
// logging into an Azure account, and running command `az account list-locations`
var azureRegions = []string{
	"eastus",
	"eastus2",
	"southcentralus",
	"westus2",
	"westus3",
	"australiaeast",
	"southeastasia",
	"northeurope",
	"swedencentral",
	"uksouth",
	"westeurope",
	"centralus",
	"northcentralus",
	"westus",
	"southafricanorth",
	"centralindia",
	"eastasia",
	"japaneast",
	"jioindiawest",
	"koreacentral",
	"canadacentral",
	"francecentral",
	"germanywestcentral",
	"norwayeast",
	"switzerlandnorth",
	"uaenorth",
	"brazilsouth",
	"centralusstage",
	"eastusstage",
	"eastus2stage",
	"northcentralusstage",
	"southcentralusstage",
	"westusstage",
	"westus2stage",
	"asia",
	"asiapacific",
	"australia",
	"brazil",
	"canada",
	"europe",
	"france",
	"germany",
	"global",
	"india",
	"japan",
	"korea",
	"norway",
	"southafrica",
	"switzerland",
	"uae",
	"uk",
	"unitedstates",
	"eastasiastage",
	"southeastasiastage",
	"centraluseuap",
	"eastus2euap",
	"westcentralus",
	"southafricawest",
	"australiacentral",
	"australiacentral2",
	"australiasoutheast",
	"japanwest",
	"jioindiacentral",
	"koreasouth",
	"southindia",
	"westindia",
	"canadaeast",
	"francesouth",
	"germanynorth",
	"norwaywest",
	"switzerlandwest",
	"ukwest",
	"uaecentral",
	"brazilsoutheast",
	"usgovarizona",
	"usgoviowa",
	"usgovvirginia",
	"usgovtexas",
}

type regionParts []string

func (r regionParts) String() string {
	var result string
	for _, p := range r {
		result += p
	}
	return result
}

func getRegions(service string, az *Azure, subscriptionID string) (map[string]string, error) {

	subscriptionsClient, providersClient, err := az.getProviders(az, subscriptionID)
	if err != nil {
		return nil, err
	}

	allLocations := make(map[string]string)
	supLocations := make(map[string]string)

	listLocationPager := subscriptionsClient.NewListLocationsPager(subscriptionID, nil)

	for listLocationPager.More() {
		// retrieve all locations for the subscription id (some of them may not be supported by the required provider)
		locationsPage, err := listLocationPager.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("failed to get locations for subscription %q: %w", subscriptionID, err)
		}

		// fill up the map: DisplayName - > Name
		for _, loc := range locationsPage.Value {
			allLocations[*loc.DisplayName] = *loc.Name
		}
	}

	// identify supported locations for the namespace and resource type
	const (
		providerNamespaceForCompute = "Microsoft.Compute"
		resourceTypeForCompute      = "locations/vmSizes"
		providerNamespaceForAks     = "Microsoft.ContainerService"
		resourceTypeForAks          = "managedClusters"
	)

	switch service {
	case "aks":
		if providers, err := providersClient.Get(context.TODO(), providerNamespaceForAks, nil); err == nil {
			for _, pr := range providers.ResourceTypes {
				if *pr.ResourceType == resourceTypeForAks {
					for _, displName := range pr.Locations {
						if loc, ok := allLocations[*displName]; ok {
							supLocations[loc] = *displName
						} else {
							log.Warnf("unsupported cloud region %q", loc)
						}
					}
					break
				}
			}
		} else {
			return nil, err
		}
		return supLocations, nil
	default:
		if providers, err := providersClient.Get(context.TODO(), providerNamespaceForCompute, nil); err == nil {
			for _, pr := range providers.ResourceTypes {
				if *pr.ResourceType == resourceTypeForCompute {
					for _, displName := range pr.Locations {
						if loc, ok := allLocations[*displName]; ok {
							supLocations[loc] = *displName
						} else {
							log.Warnf("unsupported cloud region %q", loc)
						}
					}
					break
				}
			}
		} else {
			return nil, err
		}

		return supLocations, nil
	}
}

func getRetailPrice(region string, skuName string, currencyCode string, spot bool) (string, error) {
	pricingURL := "https://prices.azure.com/api/retail/prices?$skip=0"

	if currencyCode != "" {
		pricingURL += fmt.Sprintf("&currencyCode='%s'", currencyCode)
	}

	var filterParams []string

	if region != "" {
		regionParam := fmt.Sprintf("armRegionName eq '%s'", region)
		filterParams = append(filterParams, regionParam)
	}

	if skuName != "" {
		skuNameParam := fmt.Sprintf("armSkuName eq '%s'", skuName)
		filterParams = append(filterParams, skuNameParam)
	}

	if len(filterParams) > 0 {
		filterParamsEscaped := url.QueryEscape(strings.Join(filterParams[:], " and "))
		pricingURL += fmt.Sprintf("&$filter=%s", filterParamsEscaped)
	}

	log.Infof("starting download retail price payload from \"%s\"", pricingURL)
	resp, err := http.Get(pricingURL)

	if err != nil {
		return "", fmt.Errorf("bogus fetch of \"%s\": %v", pricingURL, err)
	}

	if resp.StatusCode < 200 && resp.StatusCode > 299 {
		return "", fmt.Errorf("retail price responded with error status code %d", resp.StatusCode)
	}

	pricingPayload := AzureRetailPricing{}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error getting response: %v", err)
	}

	jsonErr := json.Unmarshal(body, &pricingPayload)
	if jsonErr != nil {
		return "", fmt.Errorf("error unmarshalling data: %v", jsonErr)
	}

	retailPrice := ""
	for _, item := range pricingPayload.Items {
		if item.Type == "Consumption" && !strings.Contains(item.ProductName, "Windows") {
			// if spot is true SkuName should contain "spot, if it is false it should not
			if spot == strings.Contains(strings.ToLower(item.SkuName), " spot") {
				retailPrice = fmt.Sprintf("%f", item.RetailPrice)
			}
		}
	}

	log.DedupedInfof(5, "done parsing retail price payload from \"%s\"\n", pricingURL)

	if retailPrice == "" {
		return retailPrice, fmt.Errorf("couldn't find price for product \"%s\" in \"%s\" region", skuName, region)
	}

	return retailPrice, nil
}

func toRegionID(meterRegion string, regions map[string]string) (string, error) {
	var rp regionParts = strings.Split(strings.ToLower(meterRegion), " ")
	regionCode := regionCodeMappings[rp[0]]
	lastPart := rp[len(rp)-1]
	var regionIds []string
	if regionID, ok := regionIdByDisplayName[meterRegion]; ok {
		regionIds = []string{
			regionID,
		}
	} else if _, err := strconv.Atoi(lastPart); err == nil {
		regionIds = []string{
			fmt.Sprintf("%s%s%s", regionCode, rp[1:len(rp)-1], lastPart),
			fmt.Sprintf("%s%s%s", rp[1:len(rp)-1], regionCode, lastPart),
		}
	} else {
		regionIds = []string{
			fmt.Sprintf("%s%s", regionCode, rp[1:]),
			fmt.Sprintf("%s%s", rp[1:], regionCode),
		}
	}
	for _, regionID := range regionIds {
		if checkRegionID(regionID, regions) {
			return regionID, nil
		}
	}
	return "", fmt.Errorf("couldn't find region %q", meterRegion)
}

// azure has very inconsistent naming standards between display names from the rate card api and display names from the regions api
// this map is to connect display names from the ratecard api to the appropriate id.
var regionIdByDisplayName = map[string]string{
	"US Gov AZ": "usgovarizona",
	"US Gov TX": "usgovtexas",
	"US Gov":    "usgovvirginia",
}

func checkRegionID(regionID string, regions map[string]string) bool {
	for region := range regions {
		if regionID == region {
			return true
		}
	}
	return false
}

// AzureRetailPricing struct for unmarshalling Azure Retail pricing api JSON response
type AzureRetailPricing struct {
	BillingCurrency    string                         `json:"BillingCurrency"`
	CustomerEntityId   string                         `json:"CustomerEntityId"`
	CustomerEntityType string                         `json:"CustomerEntityType"`
	Items              []AzureRetailPricingAttributes `json:"Items"`
	NextPageLink       string                         `json:"NextPageLink"`
	Count              int                            `json:"Count"`
}

// AzureRetailPricingAttributes struct for unmarshalling Azure Retail pricing api JSON response
type AzureRetailPricingAttributes struct {
	CurrencyCode         string     `json:"currencyCode"`
	TierMinimumUnits     float32    `json:"tierMinimumUnits"`
	RetailPrice          float32    `json:"retailPrice"`
	UnitPrice            float32    `json:"unitPrice"`
	ArmRegionName        string     `json:"armRegionName"`
	Location             string     `json:"location"`
	EffectiveStartDate   *time.Time `json:"effectiveStartDate"`
	EffectiveEndDate     *time.Time `json:"effectiveEndDate"`
	MeterId              string     `json:"meterId"`
	MeterName            string     `json:"meterName"`
	ProductId            string     `json:"productId"`
	SkuId                string     `json:"skuId"`
	ProductName          string     `json:"productName"`
	SkuName              string     `json:"skuName"`
	ServiceName          string     `json:"serviceName"`
	ServiceId            string     `json:"serviceId"`
	ServiceFamily        string     `json:"serviceFamily"`
	UnitOfMeasure        string     `json:"unitOfMeasure"`
	Type                 string     `json:"type"`
	IsPrimaryMeterRegion bool       `json:"isPrimaryMeterRegion"`
	ArmSkuName           string     `json:"armSkuName"`
}

// AzurePricing either contains a Node or PV
type AzurePricing struct {
	Node *models.Node
	PV   *models.PV
}

type Azure struct {
	Pricing                 map[string]*AzurePricing
	DownloadPricingDataLock sync.RWMutex
	Clientset               clustercache.ClusterCache
	Config                  models.ProviderConfig
	ServiceAccountChecks    *models.ServiceAccountChecks
	ClusterAccountID        string
	ClusterRegion           string

	pricingSource                     string
	consumptionPricesheetPricingError error
	priceSheetPricingError            error
	loadedAzureSecret                 bool
	azureSecret                       *AzureServiceKey
	loadedAzureStorageConfigSecret    bool
	azureStorageConfig                *AzureStorageConfig
	//getProviders				   				(*armsubscriptions.Client,*armresources.ProvidersClient, error)
}

// PricingSourceSummary returns the pricing source summary for the provider.
// The summary represents what was _parsed_ from the pricing source, not
// everything that was _available_ in the pricing source.
func (az *Azure) PricingSourceSummary() interface{} {
	return az.Pricing
}

type azureKey struct {
	Labels        map[string]string
	GPULabel      string
	GPULabelValue string
}

func (k *azureKey) Features() string {
	r, _ := util.GetRegion(k.Labels)
	region := strings.ToLower(r)
	os, _ := util.GetOperatingSystem(k.Labels)
	instance, _ := util.GetInstanceType(k.Labels)
	usageType := "ondemand"
	if os != "windows" {
		return fmt.Sprintf("%s,%s,%s", region, instance, usageType)
	} else {
		winInstance := instance + "_" + os
		return fmt.Sprintf("%s,%s,%s", region, winInstance, usageType)
	}
}

func (k *azureKey) GPUCount() int {
	return 0
}

// GPUType returns value of GPULabel if present
func (k *azureKey) GPUType() string {
	if t, ok := k.Labels[k.GPULabel]; ok {
		return t
	}
	return ""
}

func (k *azureKey) isValidGPUNode() bool {
	return k.GPUType() == k.GPULabelValue && k.GetGPUCount() != "0"
}

func (k *azureKey) ID() string {
	return ""
}

func (k *azureKey) GetGPUCount() string {
	instance, _ := util.GetInstanceType(k.Labels)
	// Double digits that could get matches lower in logic
	if strings.Contains(instance, "NC64") {
		return "4"
	}
	if strings.Contains(instance, "ND96") ||
		strings.Contains(instance, "ND40") {
		return "8"
	}

	// Ordered asc because of some series have different gpu counts on different versions
	if strings.Contains(instance, "NC6") ||
		strings.Contains(instance, "NC4") ||
		strings.Contains(instance, "NC8") ||
		strings.Contains(instance, "NC16") ||
		strings.Contains(instance, "ND6") ||
		strings.Contains(instance, "NV12s") ||
		strings.Contains(instance, "NV6") {
		return "1"
	}

	if strings.Contains(instance, "NC12") ||
		strings.Contains(instance, "ND12") ||
		strings.Contains(instance, "NV24s") ||
		strings.Contains(instance, "NV12") {
		return "2"
	}
	if strings.Contains(instance, "NC24") ||
		strings.Contains(instance, "ND24") ||
		strings.Contains(instance, "NV48s") ||
		strings.Contains(instance, "NV24") {
		return "4"
	}
	return "0"
}

// AzureStorageConfig Represents an azure storage config
// Deprecated: v1.104 Use StorageConfiguration instead
type AzureStorageConfig struct {
	SubscriptionId string `json:"azureSubscriptionID"`
	AccountName    string `json:"azureStorageAccount"`
	AccessKey      string `json:"azureStorageAccessKey"`
	ContainerName  string `json:"azureStorageContainer"`
	ContainerPath  string `json:"azureContainerPath"`
	AzureCloud     string `json:"azureCloud"`
}

// IsEmpty returns true if all fields in config are empty, false if not.
func (asc *AzureStorageConfig) IsEmpty() bool {
	return asc.SubscriptionId == "" &&
		asc.AccountName == "" &&
		asc.AccessKey == "" &&
		asc.ContainerName == "" &&
		asc.ContainerPath == "" &&
		asc.AzureCloud == ""
}

// Represents an azure app key
type AzureAppKey struct {
	AppID       string `json:"appId"`
	DisplayName string `json:"displayName"`
	Name        string `json:"name"`
	Password    string `json:"password"`
	Tenant      string `json:"tenant"`
}

// AzureServiceKey service key for a specific subscription
// Deprecated: v1.104 Use ServiceKey instead
type AzureServiceKey struct {
	SubscriptionID string       `json:"subscriptionId"`
	ServiceKey     *AzureAppKey `json:"serviceKey"`
}

// Validity check on service key
func (ask *AzureServiceKey) IsValid() bool {
	return ask.SubscriptionID != "" &&
		ask.ServiceKey != nil &&
		ask.ServiceKey.AppID != "" &&
		ask.ServiceKey.Password != "" &&
		ask.ServiceKey.Tenant != ""
}

// Loads the azure authentication via configuration or a secret set at install time.
func (az *Azure) getAzureRateCardAuth(forceReload bool, cp *models.CustomPricing) (subscriptionID, clientID, clientSecret, tenantID string) {
	// 1. Check for secret (secret values will always be used if they are present)
	s, _ := az.loadAzureAuthSecret(forceReload)
	if s != nil && s.IsValid() {
		subscriptionID = s.SubscriptionID
		clientID = s.ServiceKey.AppID
		clientSecret = s.ServiceKey.Password
		tenantID = s.ServiceKey.Tenant
		return
	}
	// 2. Check config values (set though endpoint)
	if cp.AzureSubscriptionID != "" && cp.AzureClientID != "" && cp.AzureClientSecret != "" && cp.AzureTenantID != "" {
		subscriptionID = cp.AzureSubscriptionID
		clientID = cp.AzureClientID
		clientSecret = cp.AzureClientSecret
		tenantID = cp.AzureTenantID
		return
	}
	// 3. Check if AzureSubscriptionID is set in config (set though endpoint)
	// MSI credentials will be attempted if the subscription ID is set, but clientID, clientSecret and tenantID are not
	if cp.AzureSubscriptionID != "" {
		subscriptionID = cp.AzureSubscriptionID
		return
	}
	// 4. Empty values
	return "", "", "", ""

}

// GetAzureStorageConfig retrieves storage config from secret and sets default values
func (az *Azure) GetAzureStorageConfig(forceReload bool, cp *models.CustomPricing) (*AzureStorageConfig, error) {
	// default subscription id
	defaultSubscriptionID := cp.AzureSubscriptionID

	// 1. Check Config for storage set up
	asc := &AzureStorageConfig{
		SubscriptionId: cp.AzureStorageSubscriptionID,
		AccountName:    cp.AzureStorageAccount,
		AccessKey:      cp.AzureStorageAccessKey,
		ContainerName:  cp.AzureStorageContainer,
		ContainerPath:  cp.AzureContainerPath,
		AzureCloud:     cp.AzureCloud,
	}

	// check for required fields
	if asc != nil && asc.AccessKey != "" && asc.AccountName != "" && asc.ContainerName != "" && asc.SubscriptionId != "" {
		az.ServiceAccountChecks.Set("hasStorage", &models.ServiceAccountCheck{
			Message: "Azure Storage Config exists",
			Status:  true,
		})
		return asc, nil
	}

	// 2. Check for secret
	asc, err := az.loadAzureStorageConfig(forceReload)
	if err != nil {
		log.Errorf("Error, %s", err.Error())
	} else if asc != nil {
		// To support already configured users, subscriptionID may not be set in secret in which case, the subscriptionID
		// for the rate card API is used
		if asc.SubscriptionId == "" {
			asc.SubscriptionId = defaultSubscriptionID
		}
		// check for required fields
		if asc.AccessKey != "" && asc.AccountName != "" && asc.ContainerName != "" && asc.SubscriptionId != "" {
			az.ServiceAccountChecks.Set("hasStorage", &models.ServiceAccountCheck{
				Message: "Azure Storage Config exists",
				Status:  true,
			})

			return asc, nil
		}
	}

	az.ServiceAccountChecks.Set("hasStorage", &models.ServiceAccountCheck{
		Message: "Azure Storage Config exists",
		Status:  false,
	})
	return nil, fmt.Errorf("azure storage config not found")

}

// Load once and cache the result (even on failure). This is an install time secret, so
// we don't expect the secret to change. If it does, however, we can force reload using
// the input parameter.
func (az *Azure) loadAzureAuthSecret(force bool) (*AzureServiceKey, error) {
	if !force && az.loadedAzureSecret {
		return az.azureSecret, nil
	}
	az.loadedAzureSecret = true

	exists, err := fileutil.FileExists(models.AuthSecretPath)
	if !exists || err != nil {
		return nil, fmt.Errorf("failed to locate service account file: %s", models.AuthSecretPath)
	}

	result, err := os.ReadFile(models.AuthSecretPath)
	if err != nil {
		return nil, err
	}

	var ask AzureServiceKey
	err = json.Unmarshal(result, &ask)
	if err != nil {
		return nil, err
	}

	az.azureSecret = &ask
	return &ask, nil
}

// Load once and cache the result (even on failure). This is an install time secret, so
// we don't expect the secret to change. If it does, however, we can force reload using
// the input parameter.
func (az *Azure) loadAzureStorageConfig(force bool) (*AzureStorageConfig, error) {
	if !force && az.loadedAzureStorageConfigSecret {
		return az.azureStorageConfig, nil
	}
	az.loadedAzureStorageConfigSecret = true

	exists, err := fileutil.FileExists(models.StorageConfigSecretPath)
	if !exists || err != nil {
		return nil, fmt.Errorf("failed to locate azure storage config file: %s", models.StorageConfigSecretPath)
	}

	result, err := os.ReadFile(models.StorageConfigSecretPath)
	if err != nil {
		return nil, err
	}

	var asc AzureStorageConfig
	err = json.Unmarshal(result, &asc)
	if err != nil {
		return nil, err
	}

	az.azureStorageConfig = &asc
	return &asc, nil
}

func (az *Azure) GetKey(labels map[string]string, n *v1.Node) models.Key {
	cfg, err := az.GetConfig()
	if err != nil {
		log.Infof("Error loading azure custom pricing information")
	}
	// azure defaults, see https://docs.microsoft.com/en-us/azure/aks/gpu-cluster
	gpuLabel := "accelerator"
	gpuLabelValue := "nvidia"
	if cfg.GpuLabel != "" {
		gpuLabel = cfg.GpuLabel
	}
	if cfg.GpuLabelValue != "" {
		gpuLabelValue = cfg.GpuLabelValue
	}
	return &azureKey{
		Labels:        labels,
		GPULabel:      gpuLabel,
		GPULabelValue: gpuLabelValue,
	}
}

// CreateString builds strings effectively
func createString(keys ...string) string {
	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
	}
	return b.String()
}

func transformMachineType(subCategory string, mt []string) []string {
	switch {
	case strings.Contains(subCategory, "Basic"):
		return []string{createString("Basic_", mt[0])}
	case len(mt) == 2:
		return []string{createString("Standard_", mt[0]), createString("Standard_", mt[1])}
	default:
		return []string{createString("Standard_", mt[0])}
	}
}

// func addSuffix(mt string, suffixes ...string) []string {
// 	result := make([]string, len(suffixes))
// 	var suffix string
// 	parts := strings.Split(mt, "_")
// 	if len(parts) > 2 {
// 		for _, p := range parts[2:] {
// 			suffix = createString(suffix, "_", p)
// 		}
// 	}
// 	for i, s := range suffixes {
// 		result[i] = createString(parts[0], "_", parts[1], s, suffix)
// 	}
// 	return result
// }

// func getMachineTypeVariants(mt string) []string {
// 	switch {
// 	case mtStandardB.MatchString(mt):
// 		return []string{createString(mt, "s")}
// 	case mtStandardD.MatchString(mt):
// 		var result []string
// 		result = append(result, addSuffix(mt, "s")[0])
// 		dsType := strings.Replace(mt, "Standard_D", "Standard_DS", -1)
// 		result = append(result, dsType)
// 		result = append(result, addSuffix(dsType, "-1", "-2", "-4", "-8")...)
// 		return result
// 	case mtStandardE.MatchString(mt):
// 		return addSuffix(mt, "s", "-2s", "-4s", "-8s", "-16s", "-32s")
// 	case mtStandardF.MatchString(mt):
// 		return addSuffix(mt, "s")
// 	case mtStandardG.MatchString(mt):
// 		var result []string
// 		gsType := strings.Replace(mt, "Standard_G", "Standard_GS", -1)
// 		result = append(result, gsType)
// 		return append(result, addSuffix(gsType, "-4", "-8", "-16")...)
// 	case mtStandardL.MatchString(mt):
// 		return addSuffix(mt, "s")
// 	case mtStandardM.MatchString(mt) && strings.HasSuffix(mt, "ms"):
// 		base := strings.TrimSuffix(mt, "ms")
// 		return addSuffix(base, "-2ms", "-4ms", "-8ms", "-16ms", "-32ms", "-64ms")
// 	case mtStandardM.MatchString(mt) && (strings.HasSuffix(mt, "ls") || strings.HasSuffix(mt, "ts")):
// 		return []string{}
// 	case mtStandardM.MatchString(mt) && strings.HasSuffix(mt, "s"):
// 		base := strings.TrimSuffix(mt, "s")
// 		return addSuffix(base, "", "m")
// 	case mtStandardN.MatchString(mt):
// 		return addSuffix(mt, "s")
// 	}
// 	return []string{}
// }

func (az *Azure) GetManagementPlatform() (string, error) {
	nodes := az.Clientset.GetAllNodes()

	if len(nodes) > 0 {
		n := nodes[0]
		providerID := n.Spec.ProviderID
		if strings.Contains(providerID, "aks") {
			return "aks", nil
		}
	}
	return "", nil
}

// DownloadPricingData uses provided azure "best guesses" for pricing
func (az *Azure) DownloadPricingData() error {
	az.DownloadPricingDataLock.Lock()
	defer az.DownloadPricingDataLock.Unlock()

	config, err := az.GetConfig()
	if err != nil {
		az.consumptionPricesheetPricingError = err
		return err
	}

	envBillingAccount := env.GetAzureBillingAccount()
	if envBillingAccount != "" {
		config.AzureBillingAccount = envBillingAccount
	}
	envOfferID := env.GetAzureOfferID()
	if envOfferID != "" {
		config.AzureOfferDurableID = envOfferID
	}

	// Load the service provider keys
	subscriptionID, clientID, clientSecret, tenantID := az.getAzureRateCardAuth(false, config)
	config.AzureSubscriptionID = subscriptionID
	config.AzureClientID = clientID
	config.AzureClientSecret = clientSecret
	config.AzureTenantID = tenantID

	// use consumption pricesheet API
	useBillingAccounPricing = false
	results := az.getConsumptionPricesheetData(subscriptionID)
	allPrices, err := az.parseResults(config, results, config.CPU)
	if err != nil {
		az.consumptionPricesheetPricingError = err
	} else {
		az.Pricing = allPrices
		az.pricingSource = consumptionPriceSheetPricingSource
		az.consumptionPricesheetPricingError = nil
	}
	baseCPUPrice := config.CPU

	// If we've got a billing account set, kick off downloading the custom pricing data.
	if useBillingAccounPricing && config.AzureBillingAccount != "" {
		getBillingAccountPricing(config, subscriptionID, baseCPUPrice, az)
	}
	return nil
}

func getBillingAccountPricing(config *models.CustomPricing, subscriptionID string, baseCPUPrice string, az *Azure) error {

	regions, err := getRegions("compute", az, config.AzureSubscriptionID)
	if err != nil {
		log.Warnf("Error in pricing download regions from API")
		return err
	}

	// The price sheet can take 5 minutes to generate, so we don't
	// want to hang onto the lock while we're waiting for it.
	downloader := PriceSheetDownloader{
		TenantID:       config.AzureTenantID,
		ClientID:       config.AzureClientID,
		ClientSecret:   config.AzureClientSecret,
		BillingAccount: config.AzureBillingAccount,
		OfferID:        config.AzureOfferDurableID,
		ConvertMeterInfo: func(meterInfo armcommerce.MeterInfo) (map[string]*AzurePricing, error) {
			return convertMeterToPricings(meterInfo, regions, baseCPUPrice)
		},
	}

	go func() {
		ctx := context.Background()
		allPrices, err := downloader.GetPricing(ctx)

		az.DownloadPricingDataLock.Lock()
		defer az.DownloadPricingDataLock.Unlock()
		if err != nil {
			log.Errorf("Error downloading Azure price sheet: %s", err)
			az.priceSheetPricingError = err
			return
		}
		addAzureFilePricing(allPrices, regions)
		az.Pricing = allPrices
		az.pricingSource = priceSheetPricingSource
		az.priceSheetPricingError = nil
	}()
	return nil
}

func (*Azure) getProviders(az *Azure, subscriptionID string) (*armsubscriptions.Client, *armresources.ProvidersClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("creating providers client: %w", err)
	}

	options := arm.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Cloud: determineCloudByRegion(az.ClusterRegion),
		},
	}

	subscriptionsClientFactory, err := armsubscriptions.NewClientFactory(cred, &options)
	if err != nil {
		return nil, nil, fmt.Errorf("creating providers client: %w", err)
	}

	subscriptionsClient := subscriptionsClientFactory.NewClient()

	providersClient, err := armresources.NewProvidersClient(subscriptionID, cred, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("creating providers client: %w", err)
	}
	return subscriptionsClient, providersClient, nil
}

func convertMeterToPricings(info armcommerce.MeterInfo, regions map[string]string, baseCPUPrice string) (map[string]*AzurePricing, error) {
	meterName := *info.MeterName
	meterRegion := *info.MeterRegion
	meterCategory := *info.MeterCategory
	meterSubCategory := *info.MeterSubCategory

	region, err := toRegionID(meterRegion, regions)
	if err != nil {
		// Skip this meter if we don't recognize the region.
		return nil, nil
	}

	if strings.Contains(meterSubCategory, "Windows") {
		// This meter doesn't correspond to any pricings.
		return nil, nil
	}

	if strings.Contains(meterCategory, "Storage") {
		if strings.Contains(meterSubCategory, "HDD") || strings.Contains(meterSubCategory, "SSD") || strings.Contains(meterSubCategory, "Premium Files") {
			var storageClass string = ""
			if strings.Contains(meterName, "P4 ") {
				storageClass = AzureDiskPremiumSSDStorageClass
			} else if strings.Contains(meterName, "E4 ") {
				storageClass = AzureDiskStandardSSDStorageClass
			} else if strings.Contains(meterName, "S4 ") {
				storageClass = AzureDiskStandardStorageClass
			} else if strings.Contains(meterName, "ZRS ") && strings.Contains(meterSubCategory, "Standard SSD") {
				storageClass = AzureDiskStandardSSDZRSStorageClass
			} else if strings.Contains(meterName, "ZRS ") && strings.Contains(meterSubCategory, "Premium SSD") {
				storageClass = AzureDiskPremiumSSDZRSStorageClass
			} else if strings.Contains(meterName, "LRS Provisioned") {
				storageClass = AzureFilePremiumStorageClass
			}

			if storageClass != "" {
				var priceInUsd float32

				if len(info.MeterRates) < 1 {
					return nil, fmt.Errorf("missing rate info %+v", map[string]interface{}{"MeterSubCategory": *info.MeterSubCategory, "region": region})
				}
				for _, rate := range info.MeterRates {
					priceInUsd += *rate
				}
				// rate is in disk per month, resolve price per hour, then GB per hour
				pricePerHour := priceInUsd / 730.0 / 32.0
				priceStr := fmt.Sprintf("%f", pricePerHour)

				key := region + "," + storageClass
				log.Debugf("Adding PV.Key: %s, Cost: %s", key, priceStr)
				return map[string]*AzurePricing{
					key: {
						PV: &models.PV{
							Cost:   priceStr,
							Region: region,
						},
					},
				}, nil
			}
		}
	}

	if !strings.Contains(meterCategory, "Virtual Machines") {
		return nil, nil
	}

	usageType := ""
	if !strings.Contains(meterName, "Low Priority") {
		usageType = "ondemand"
	} else {
		usageType = "preemptible"
	}

	var instanceTypes []string
	name := strings.TrimSuffix(meterName, " Low Priority")
	instanceType := strings.Split(name, "/")
	for _, it := range instanceType {
		if strings.Contains(meterSubCategory, "Promo") {
			it = it + " Promo"
		}
		instanceTypes = append(instanceTypes, strings.Replace(it, " ", "_", -1))
	}

	instanceTypes = transformMachineType(meterSubCategory, instanceTypes)
	if strings.Contains(name, "Expired") {
		instanceTypes = []string{}
	}

	var priceInUsd float32

	if len(info.MeterRates) < 1 {
		return nil, fmt.Errorf("missing rate info %+v", map[string]interface{}{"MeterSubCategory": *info.MeterSubCategory, "region": region})
	}
	for _, rate := range info.MeterRates {
		priceInUsd += *rate
	}
	priceStr := fmt.Sprintf("%f", priceInUsd)
	results := make(map[string]*AzurePricing)
	for _, instanceType := range instanceTypes {

		key := fmt.Sprintf("%s,%s,%s", region, instanceType, usageType)
		pricing := &AzurePricing{
			Node: &models.Node{
				Cost:         priceStr,
				BaseCPUPrice: baseCPUPrice,
				UsageType:    usageType,
			},
		}
		results[key] = pricing
	}
	return results, nil

}

func addAzureFilePricing(prices map[string]*AzurePricing, regions map[string]string) {
	// There is no easy way of supporting Standard Azure-File, because it's billed per used GB
	// this will set the price to "0" as a workaround to not spam with `Persistent Volume pricing not found for` error
	// check https://github.com/opencost/opencost/issues/159 for more information (same problem on AWS)
	zeroPrice := "0.0"
	for region := range regions {
		key := region + "," + AzureFileStandardStorageClass
		log.Debugf("Adding PV.Key: %s, Cost: %s", key, zeroPrice)
		prices[key] = &AzurePricing{
			PV: &models.PV{
				Cost:   zeroPrice,
				Region: region,
			},
		}
	}
}

// determineCloudByRegion uses region name to pick the correct Cloud Environment for the azure provider to use
func determineCloudByRegion(region string) cloud.Configuration {
	lcRegion := strings.ToLower(region)
	if strings.Contains(lcRegion, "china") {
		return cloud.AzureChina
	}
	if strings.Contains(lcRegion, "gov") || strings.Contains(lcRegion, "dod") {
		return cloud.AzureGovernment
	}
	// Default to public cloud
	return cloud.AzurePublic
}

func (az *Azure) addPricing(features string, azurePricing *AzurePricing) {
	if az.Pricing == nil {
		az.Pricing = map[string]*AzurePricing{}
	}
	az.Pricing[features] = azurePricing
}

// AllNodePricing returns the Azure pricing objects stored
func (az *Azure) AllNodePricing() (interface{}, error) {
	az.DownloadPricingDataLock.RLock()
	defer az.DownloadPricingDataLock.RUnlock()
	return az.Pricing, nil
}

// NodePricing returns Azure pricing data for a single node
func (az *Azure) NodePricing(key models.Key) (*models.Node, error) {
	az.DownloadPricingDataLock.RLock()
	defer az.DownloadPricingDataLock.RUnlock()
	pricingDataExists := true
	if az.Pricing == nil {
		pricingDataExists = false
		log.DedupedWarningf(1, "Unable to download Azure pricing data")
	}

	azKey, ok := key.(*azureKey)
	if !ok {
		return nil, fmt.Errorf("azure: NodePricing: key is of type %T", key)
	}
	config, _ := az.GetConfig()

	// Spot Node
	if slv, ok := azKey.Labels[config.SpotLabel]; ok && slv == config.SpotLabelValue && config.SpotLabel != "" && config.SpotLabelValue != "" {
		features := strings.Split(azKey.Features(), ",")
		region := features[0]
		instance := features[1]
		spotFeatures := fmt.Sprintf("%s,%s,%s", region, instance, "spot")
		if n, ok := az.Pricing[spotFeatures]; ok {
			log.DedupedInfof(5, "Returning pricing for node %s: %+v from key %s", azKey, n, spotFeatures)
			if azKey.isValidGPUNode() {
				n.Node.GPU = "1" // TODO: support multiple GPUs
			}
			return n.Node, nil
		}
		log.Infof("[Info] found spot instance, trying to get retail price for %s: %s, ", spotFeatures, azKey)
		spotCost, err := getRetailPrice(region, instance, config.CurrencyCode, true)
		if err != nil {
			log.DedupedWarningf(5, "failed to retrieve spot retail pricing")
		} else {
			gpu := ""
			if azKey.isValidGPUNode() {
				gpu = "1"
			}
			spotNode := &models.Node{
				Cost:      spotCost,
				UsageType: "spot",
				GPU:       gpu,
			}
			az.addPricing(spotFeatures, &AzurePricing{
				Node: spotNode,
			})
			return spotNode, nil
		}
	}

	// Use the downloaded pricing data if possible. Otherwise, use default
	// configured pricing data.
	if pricingDataExists {
		if n, ok := az.Pricing[azKey.Features()]; ok {
			log.Debugf("Returning pricing for node %s: %+v from key %s", azKey, n, azKey.Features())
			if azKey.isValidGPUNode() {
				n.Node.GPU = azKey.GetGPUCount()
			}
			return n.Node, nil
		}
		log.DedupedWarningf(5, "No pricing data found for node %s from key %s", azKey, azKey.Features())
	}
	c, err := az.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("no default pricing data available")
	}

	// GPU Node
	if azKey.isValidGPUNode() {
		return &models.Node{
			VCPUCost:         c.CPU,
			RAMCost:          c.RAM,
			UsesBaseCPUPrice: true,
			GPUCost:          c.GPU,
			GPU:              azKey.GetGPUCount(),
		}, nil
	}

	// Serverless Node. This is an Azure Container Instance, and no pods can be
	// scheduled to this node. Azure does not charge for this node. Set costs to
	// zero.
	if azKey.Labels["kubernetes.io/hostname"] == "virtual-node-aci-linux" {
		return &models.Node{
			VCPUCost: "0",
			RAMCost:  "0",
		}, nil
	}

	// Regular Node
	return &models.Node{
		VCPUCost:         c.CPU,
		RAMCost:          c.RAM,
		UsesBaseCPUPrice: true,
	}, nil
}

// Stubbed NetworkPricing for Azure. Pull directly from azure.json for now
func (az *Azure) NetworkPricing() (*models.Network, error) {
	cpricing, err := az.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	znec, err := strconv.ParseFloat(cpricing.ZoneNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	rnec, err := strconv.ParseFloat(cpricing.RegionNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	inec, err := strconv.ParseFloat(cpricing.InternetNetworkEgress, 64)
	if err != nil {
		return nil, err
	}

	return &models.Network{
		ZoneNetworkEgressCost:     znec,
		RegionNetworkEgressCost:   rnec,
		InternetNetworkEgressCost: inec,
	}, nil
}

// LoadBalancerPricing on Azure, LoadBalancer services correspond to public IPs. For now the pricing of LoadBalancer
// services will be that of a standard static public IP https://azure.microsoft.com/en-us/pricing/details/ip-addresses/.
// Azure still has load balancers which follow the standard pricing scheme based on rules
// https://azure.microsoft.com/en-us/pricing/details/load-balancer/, they are created on a per-cluster basis.
func (azr *Azure) LoadBalancerPricing() (*models.LoadBalancer, error) {
	return &models.LoadBalancer{
		Cost: 0.005,
	}, nil
}

type azurePvKey struct {
	Labels                 map[string]string
	StorageClass           string
	StorageClassParameters map[string]string
	DefaultRegion          string
	ProviderId             string
}

func (az *Azure) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	providerID := ""
	if pv.Spec.AzureDisk != nil {
		providerID = pv.Spec.AzureDisk.DiskName
	}
	return &azurePvKey{
		Labels:                 pv.Labels,
		StorageClass:           pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		DefaultRegion:          defaultRegion,
		ProviderId:             providerID,
	}
}

func (key *azurePvKey) ID() string {
	return key.ProviderId
}

func (key *azurePvKey) GetStorageClass() string {
	return key.StorageClass
}

func (key *azurePvKey) Features() string {
	storageClass := key.StorageClassParameters["storageaccounttype"]
	storageSKU := key.StorageClassParameters["skuName"]
	if storageSKU == "" {
		storageSKU = key.StorageClassParameters["skuname"]
	}
	if storageClass != "" {
		if strings.EqualFold(storageClass, "Premium_LRS") {
			storageClass = AzureDiskPremiumSSDStorageClass
		} else if strings.EqualFold(storageClass, "StandardSSD_LRS") {
			storageClass = AzureDiskStandardSSDStorageClass
		} else if strings.EqualFold(storageClass, "Standard_LRS") {
			storageClass = AzureDiskStandardStorageClass
		} else if strings.EqualFold(storageClass, "StandardSSD_ZRS") {
			storageClass = AzureDiskStandardSSDZRSStorageClass
		} else if strings.EqualFold(storageClass, "PremiumSSD_ZRS") {
			storageClass = AzureDiskPremiumSSDZRSStorageClass
		}
	} else {
		if strings.EqualFold(storageSKU, "Premium_LRS") {
			storageClass = AzureFilePremiumStorageClass
		} else if strings.EqualFold(storageSKU, "Standard_LRS") {
			storageClass = AzureFileStandardStorageClass
		} else if strings.EqualFold(storageSKU, "StandardSSD_LRS") {
			storageClass = AzureDiskStandardSSDStorageClass
		}
	}
	if region, ok := util.GetRegion(key.Labels); ok {
		return region + "," + storageClass
	}

	return key.DefaultRegion + "," + storageClass
}

func (*Azure) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (az *Azure) GetDisks() ([]byte, error) {
	disks, err := az.getDisks()
	if err != nil {
		return nil, err
	}

	return json.Marshal(disks)
}

func (az *Azure) getDisks() ([]*armcompute.Disk, error) {
	config, err := az.GetConfig()
	if err != nil {
		return nil, err
	}

	// Load the service provider keys
	subscriptionID, clientID, clientSecret, tenantID := az.getAzureRateCardAuth(false, config)
	config.AzureSubscriptionID = subscriptionID
	config.AzureClientID = clientID
	config.AzureClientSecret = clientSecret
	config.AzureTenantID = tenantID

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return []*armcompute.Disk{}, fmt.Errorf("creating credential: %w", err)
	}

	client, err := armcompute.NewDisksClient(config.AzureSubscriptionID, cred, nil)
	if err != nil {
		return []*armcompute.Disk{}, fmt.Errorf("creating disks client: %w", err)
	}

	ctx := context.TODO()

	var disks []*armcompute.Disk

	listPager := client.NewListPager(nil)
	for listPager.More() {
		diskPage, err := listPager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting disks: %v", err)
		}

		for _, d := range diskPage.Value {
			d := d
			disks = append(disks, d)
		}
	}

	return disks, nil
}

func (az *Azure) isDiskOrphaned(disk *armcompute.Disk) bool {
	//TODO: needs better algorithm
	return *disk.Properties.DiskState == "Unattached" || *disk.Properties.DiskState == "Reserved"
}

func (az *Azure) GetOrphanedResources() ([]models.OrphanedResource, error) {
	disks, err := az.getDisks()
	if err != nil {
		return nil, err
	}

	var orphanedResources []models.OrphanedResource

	for _, d := range disks {
		if az.isDiskOrphaned(d) {
			cost, err := az.findCostForDisk(d)
			if err != nil {
				return nil, err
			}

			diskName := ""
			if d.Name != nil {
				diskName = *d.Name
			}

			diskRegion := ""
			if d.Location != nil {
				diskRegion = *d.Location
			}

			var diskSize int64
			if d.Properties.DiskSizeGB != nil {
				diskSize = int64(*d.Properties.DiskSizeGB)
			}

			desc := map[string]string{}
			for k, v := range d.Tags {
				if v == nil {
					desc[k] = ""
				} else {
					desc[k] = *v
				}
			}

			or := models.OrphanedResource{
				Kind:        "disk",
				Region:      diskRegion,
				Description: desc,
				Size:        &diskSize,
				DiskName:    diskName,
				MonthlyCost: &cost,
			}
			orphanedResources = append(orphanedResources, or)
		}
	}

	return orphanedResources, nil
}

func (az *Azure) findCostForDisk(d *armcompute.Disk) (float64, error) {
	if d == nil {
		return 0.0, fmt.Errorf("disk is empty")
	}
	storageClass := string(*d.SKU.Name)
	if strings.EqualFold(storageClass, "Premium_LRS") {
		storageClass = AzureDiskPremiumSSDStorageClass
	} else if strings.EqualFold(storageClass, "StandardSSD_LRS") {
		storageClass = AzureDiskStandardSSDStorageClass
	} else if strings.EqualFold(storageClass, "Standard_LRS") {
		storageClass = AzureDiskStandardStorageClass
	}

	key := *d.Location + "," + storageClass

	diskPricePerGBHour, err := strconv.ParseFloat(az.Pricing[key].PV.Cost, 64)
	if err != nil {
		return 0.0, fmt.Errorf("error converting to float: %s", err)
	}
	cost := diskPricePerGBHour * timeutil.HoursPerMonth * float64(*d.Properties.DiskSizeGB)

	return cost, nil
}

func (az *Azure) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

	m := make(map[string]string)
	m["name"] = "Azure Cluster #1"
	c, err := az.GetConfig()
	if err != nil {
		return nil, err
	}
	if c.ClusterName != "" {
		m["name"] = c.ClusterName
	}
	m["provider"] = kubecost.AzureProvider
	m["account"] = az.ClusterAccountID
	m["region"] = az.ClusterRegion
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	m["id"] = env.GetClusterID()
	return m, nil

}

func (az *Azure) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return az.Config.UpdateFromMap(a)
}

func (az *Azure) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return az.Config.Update(func(c *models.CustomPricing) error {
		if updateType == AzureStorageUpdateType {
			asc := &AzureStorageConfig{}
			err := json.NewDecoder(r).Decode(&asc)
			if err != nil {
				return fmt.Errorf("error decoding AzureStorageConfig: %s", err)
			}

			c.AzureStorageSubscriptionID = asc.SubscriptionId
			c.AzureStorageAccount = asc.AccountName
			if asc.AccessKey != "" {
				c.AzureStorageAccessKey = asc.AccessKey
			}
			c.AzureStorageContainer = asc.ContainerName
			c.AzureContainerPath = asc.ContainerPath
			c.AzureCloud = asc.AzureCloud
		} else {
			// This will block if not in a goroutine. It calls GetConfig(), which
			// in turn calls GetCustomPricingData, which acquires the same lock
			// that is acquired by az.Config.Update, which is the function to
			// which this function gets passed, and subsequently called. Booo.
			defer func() {
				go az.DownloadPricingData()
			}()

			a := make(map[string]interface{})
			err := json.NewDecoder(r).Decode(&a)
			if err != nil {
				return fmt.Errorf("error decoding AzureStorageConfig: %s", err)
			}

			for k, v := range a {
				// Just so we consistently supply / receive the same values, uppercase the first letter.
				kUpper := utils.ToTitle.String(k)
				vstr, ok := v.(string)
				if ok {
					err := models.SetCustomPricingField(c, kUpper, vstr)
					if err != nil {
						return fmt.Errorf("error setting custom pricing field on AzureStorageConfig: %s", err)
					}
				} else {
					return fmt.Errorf("type error while updating config for %s", kUpper)
				}
			}
		}

		if env.IsRemoteEnabled() {
			err := utils.UpdateClusterMeta(env.GetClusterID(), c.ClusterName)
			if err != nil {
				return fmt.Errorf("error updating cluster metadata: %s", err)
			}
		}

		return nil
	})
}

func (az *Azure) GetConfig() (*models.CustomPricing, error) {
	c, err := az.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if c.Discount == "" {
		c.Discount = "0%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.CurrencyCode == "" {
		c.CurrencyCode = "USD"
	}
	if c.AzureBillingRegion == "" {
		c.AzureBillingRegion = "US"
	}
	// Default to pay-as-you-go Durable offer id
	if c.AzureOfferDurableID == "" {
		c.AzureOfferDurableID = "MS-AZR-0003p"
	}
	if c.ShareTenancyCosts == "" {
		c.ShareTenancyCosts = models.DefaultShareTenancyCost
	}
	if c.SpotLabel == "" {
		c.SpotLabel = defaultSpotLabel
	}
	if c.SpotLabelValue == "" {
		c.SpotLabelValue = defaultSpotLabelValue
	}
	return c, nil
}

func (az *Azure) ApplyReservedInstancePricing(nodes map[string]*models.Node) {

}

func (az *Azure) PVPricing(pvk models.PVKey) (*models.PV, error) {
	az.DownloadPricingDataLock.RLock()
	defer az.DownloadPricingDataLock.RUnlock()

	pricing, ok := az.Pricing[pvk.Features()]
	if !ok {
		log.Debugf("Persistent Volume pricing not found for %s: %s", pvk.GetStorageClass(), pvk.Features())
		return &models.PV{}, nil
	}
	return pricing.PV, nil
}

func (az *Azure) GetLocalStorageQuery(window, offset time.Duration, rate bool, used bool) string {
	return ""
}

func (az *Azure) ServiceAccountStatus() *models.ServiceAccountStatus {
	return az.ServiceAccountChecks.GetStatus()
}

const (
	rateCardPricingSource              = "Rate Card API"
	consumptionPriceSheetPricingSource = "Consumption Pricesheet API"
	priceSheetPricingSource            = "Price Sheet API"
)

// PricingSourceStatus returns the status of the rate card api
func (az *Azure) PricingSourceStatus() map[string]*models.PricingSource {
	az.DownloadPricingDataLock.Lock()
	defer az.DownloadPricingDataLock.Unlock()
	sources := make(map[string]*models.PricingSource)
	errMsg := ""
	if az.consumptionPricesheetPricingError != nil {
		errMsg = az.consumptionPricesheetPricingError.Error()
	}
	rcps := &models.PricingSource{
		Name:    rateCardPricingSource,
		Enabled: az.pricingSource == consumptionPriceSheetPricingSource,
		Error:   errMsg,
	}
	if rcps.Error != "" {
		rcps.Available = false
	} else if len(az.Pricing) == 0 {
		rcps.Error = "No Pricing Data Available"
		rcps.Available = false
	} else {
		rcps.Available = true
	}

	errMsg = ""
	if az.priceSheetPricingError != nil {
		errMsg = az.priceSheetPricingError.Error()
	}
	psps := &models.PricingSource{
		Name:    priceSheetPricingSource,
		Enabled: az.pricingSource == priceSheetPricingSource,
		Error:   errMsg,
	}
	if psps.Error != "" {
		psps.Available = false
	} else if len(az.Pricing) == 0 {
		psps.Error = "No Pricing Data Available"
		psps.Available = false
	} else if env.GetAzureBillingAccount() == "" {
		psps.Error = "No Azure Billing Account ID"
		psps.Available = false
	} else {
		psps.Available = true
	}
	sources[rateCardPricingSource] = rcps
	sources[priceSheetPricingSource] = psps
	return sources
}

func (*Azure) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (az *Azure) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (az *Azure) Regions() []string {

	regionOverrides := env.GetRegionOverrideList()

	if len(regionOverrides) > 0 {
		log.Debugf("Overriding Azure regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}

	return azureRegions
}

func ParseAzureSubscriptionID(id string) string {
	match := azureSubRegex.FindStringSubmatch(id)
	if len(match) >= 2 {
		return match[1]
	}
	// Return empty string if an account could not be parsed from provided string
	return ""
}
