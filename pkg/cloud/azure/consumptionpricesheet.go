// Gets paginated request from Microsoft Consumption Pricesheet API, unmarshals the result
// into the pricesheet JSON model, and writes each page to a file in a folder called output.

package azure

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/consumption/armconsumption"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/log"
)

func (az *Azure) getConsumptionPricesheetData(subscriptionID string) map[int]armconsumption.PriceSheetClientGetResponse {
	allPricesheetPages, err := handlePricesheetRequest(subscriptionID)
	if err != nil {
		log.Errorf("Consumption Pricesheet API: %v", err)
		return nil
	}
	return allPricesheetPages
}

func handlePricesheetRequest(subscriptionID string) (pricing map[int]armconsumption.PriceSheetClientGetResponse, err error) {
	allPrices := make(map[int]armconsumption.PriceSheetClientGetResponse)
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("Consumption Pricesheet API: failed to obtain Azure credential: %v", err)
	}
	ctx := context.Background()
	retryPolicy := policy.RetryOptions{
		RetryDelay:    time.Duration(10 * time.Second),
		MaxRetryDelay: time.Duration(320 * time.Second), // API can throttle
	}
	opts := policy.ClientOptions{
		Retry: retryPolicy,
	}
	clientFactory, err := armconsumption.NewClientFactory(subscriptionID, cred, &arm.ClientOptions{
		ClientOptions: opts,
	})
	if err != nil {
		return nil, fmt.Errorf("consumption Pricesheet API: failed to create pricesheet client: %v", err)
	}
	expand := "properties/meterDetails"
	client := clientFactory.NewPriceSheetClient()
	res, err := client.Get(ctx, &armconsumption.PriceSheetClientGetOptions{
		Expand:    &expand,
		Skiptoken: nil,
		Top:       nil,
	})
	if err != nil {
		return nil, fmt.Errorf("consumption Pricesheet API: failed to complete http request: %v", err)
	}
	counter := 1
	allPrices[counter] = res
	nextLink := res.PriceSheetResult.Properties.NextLink
	for nextLink != nil && *nextLink != "" {
		if counter%10 == 0 {
			log.Infof("Consumption Pricesheet API: %v pages retrieved", counter)
		}
		skipToken := getSkipToken(nextLink)
		res, err := client.Get(ctx, &armconsumption.PriceSheetClientGetOptions{
			Expand:    &expand,
			Skiptoken: &skipToken,
			Top:       nil,
		})
		if err != nil {
			return nil, fmt.Errorf("consumption Pricesheet API: failed to finish request for page %v: %v", counter, err)
		}
		counter++
		allPrices[counter] = res
		nextLink = res.PriceSheetResult.Properties.NextLink
	}
	log.Infof("Consumption Pricesheet API: %v requests completed", counter)
	return allPrices, nil
}

func getSkipToken(nextLink *string) string {
	linkParts := strings.Split(*nextLink, "skiptoken=")
	params := linkParts[1]
	paramParts := strings.Split(params, "&")
	skipToken := paramParts[0]
	return skipToken
}

// Determine the divisor that converts the pricing to an hourly cost.
// For storage -- rate is in disk per month, resolve price per hour, then GB per hour
func getUnitFromString(unitString string) float64 {
	unitParts := strings.Split(unitString, " ")
	// handle known bad formatting in data
	if len(unitParts) == 1 && unitParts[0] == "1/Month" {
		unitParts = []string{"1", "Month"}
	}
	unit, err := strconv.ParseFloat(unitParts[0], 64)
	if err != nil {
		log.Errorf("Consumption Pricesheet API: no numeric value for unit %v: using 1", unitString)
		unit = 1
	}
	if len(unitParts) == 1 {
		return unit
	}
	if len(unitParts) == 2 {
		if unitParts[1] != "Hour" && unitParts[1] != "Hours" &&
			unitParts[1] != "/Hour" && unitParts[1] != "/Month" &&
			unitParts[1] != "GB/Month" && unitParts[1] != "PiB/Hour" &&
			unitParts[1] != "TiB/Hour" {
			log.Infof("Consumption Pricesheet API: got unexpected unit measure: %v", unitParts[1])
		}
		// TODO confifm conversions - do we need to break down to GB or KB per hour (for ex) or is hourly cost sufficient?
		measure := unitParts[1]
		switch measure {
		case "Hour", "Hours", "Tib/Hour", "PiB/Hour", "GB/Hour":
			return unit
		case "GB/Month":
			return unit * 730 * 32
		case "/Month":
			return unit * 730
		default:
			return unit
		}
	}
	// Currently no matches for PV and Node data, but this case exists in the larger dataset
	if len(unitParts) > 2 {
		log.Infof("Consumption Pricesheet API: TODO: handle unit string %v", unitString)
	}
	return unit
}

// Iterate on the paged results returned by the Consumption Pricesheet API to extract cost data for relevant meter types.
func (az *Azure) parseResults(config *models.CustomPricing, results map[int]armconsumption.PriceSheetClientGetResponse, baseCPUPrice string) (map[string]*AzurePricing, error) {
	regions, err := getRegions("compute", az, config.AzureSubscriptionID)
	if err != nil {
		return nil, err
	}
	allPrices := make(map[string]*AzurePricing)
	for _, pricesheet := range results {
		pricesheets := pricesheet.Properties.Pricesheets
		for i := 0; i < len(pricesheets); i++ {
			meters := az.processMeterData(pricesheets[i], regions, baseCPUPrice)
			for key, pricing := range meters {
				allPrices[key] = pricing

			}
		}
	}
	log.Infof("Consumption Pricesheet API: parsed %v results pages", len(results))
	return allPrices, nil
}

// Populate the pricing model with Storage and Node cost data from the Consumption Pricesheet API.
func (*Azure) processMeterData(pricedItem *armconsumption.PriceSheetProperties, regions map[string]string, baseCPUPrice string) map[string]*AzurePricing {
	if pricedItem.MeterDetails == nil {
		// Without meter details there's no way to match the meter type
		log.Infof("Consumption Pricesheet API: skipping meter ID %v, no meter details", *pricedItem.MeterID)
		return nil
	}
	meterName := *pricedItem.MeterDetails.MeterName
	meterRegion := *pricedItem.MeterDetails.MeterLocation
	meterCategory := *pricedItem.MeterDetails.MeterCategory
	meterSubCategory := *pricedItem.MeterDetails.MeterSubCategory
	currency := *pricedItem.CurrencyCode

	region, err := toRegionID(meterRegion, regions)
	if err != nil {
		// Skip this meter if we don't recognize the region.
		return nil
	}

	if strings.Contains(meterSubCategory, "Windows") {
		// Supporting *only* Windows VM types known to be used by LightOps
		if (strings.Contains(*pricedItem.MeterDetails.MeterName, "D16s v3") ||
			strings.Contains(*pricedItem.MeterDetails.MeterName, "D8s v3")) &&
			!strings.Contains(*pricedItem.MeterDetails.MeterName, "Low Priority") &&
			*pricedItem.MeterDetails.MeterLocation == "EU West" {

			unitPrice := getHourlyUSDPrice(pricedItem)
			priceStr := fmt.Sprintf("%f", unitPrice)
			usageType := "ondemand"
			instanceTypes := getInstanceTypes(meterName, meterSubCategory)
			results := make(map[string]*AzurePricing)
			for _, instanceType := range instanceTypes {
				instanceType = instanceType + "_" + "windows"
				key := fmt.Sprintf("%s,%s,%s", region, instanceType, usageType)
				pricing := &AzurePricing{
					Node: &models.Node{
						Cost:         priceStr,
						BaseCPUPrice: baseCPUPrice,
						UsageType:    usageType,
						Currency:     currency,
					},
				}
				log.Debugf("Adding Node.Key: %s, Cost: %s", key, priceStr)
				results[key] = pricing
			}
			return results
		}
		return nil
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
				// get pricing for Storage types
				unitPrice := getHourlyUSDPrice(pricedItem)
				priceStr := fmt.Sprintf("%f", unitPrice)

				key := region + "," + storageClass

				log.Debugf("Adding PV.Key: %s, Cost: %s", key, priceStr)
				return map[string]*AzurePricing{
					key: {
						PV: &models.PV{
							Cost:     priceStr,
							Region:   region,
							Currency: currency,
						},
					},
				}
			}
		}
	}

	if !strings.Contains(meterCategory, "Virtual Machines") {
		return nil
	}

	// get pricing for only remaining types that weren't filtered out
	unitPrice := getHourlyUSDPrice(pricedItem)
	priceStr := fmt.Sprintf("%f", unitPrice)

	usageType := ""
	if !strings.Contains(meterName, "Low Priority") {
		usageType = "ondemand"
	} else {
		usageType = "preemptible"
	}

	instanceTypes := getInstanceTypes(meterName, meterSubCategory)

	results := make(map[string]*AzurePricing)
	for _, instanceType := range instanceTypes {
		key := fmt.Sprintf("%s,%s,%s", region, instanceType, usageType)
		pricing := &AzurePricing{
			Node: &models.Node{
				Cost:         priceStr,
				BaseCPUPrice: baseCPUPrice,
				UsageType:    usageType,
				Currency:     currency,
			},
		}
		log.Debugf("Adding Node.Key: %s, Cost: %s", key, priceStr)
		results[key] = pricing
	}
	return results
}

func getInstanceTypes(meterName string, meterSubCategory string) []string {
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
	return instanceTypes
}

// Get USD pricing.
func getHourlyUSDPrice(pricedItem *armconsumption.PriceSheetProperties) float64 {
	unitDenominator := getUnitFromString(*pricedItem.MeterDetails.Unit)
	unitPrice := *pricedItem.UnitPrice / float64(unitDenominator)
	currency := *pricedItem.CurrencyCode
	switch currency {
	case "EUR":
		unitPrice = euroToUsd(unitPrice)
	case "USD":
		// no conversion needed
	default:
		log.Infof("Consumption Pricesheet API: no conversion for currency %v", currency)
	}
	return unitPrice
}

// For the past year (as of 10/23), the conversion rate ranged from 0.97 to 1.12
// TODO investigate other options for handlng conversion.
func euroToUsd(euro float64) float64 {
	var exchangeRate float64
	rate := env.GetEuroToUSDExchangeRate()
	var err error
	fallbackRate := 1.6
	if rate != "" {
		exchangeRate, err = strconv.ParseFloat(rate, 64)
		if err != nil {
			exchangeRate = fallbackRate
			log.Infof("Consumption Pricesheet API: failed to convert env var EURO_TO_USD_EXCHANGE_RATE to float: using rate of %v", fallbackRate)
		}
	} else {
		exchangeRate = fallbackRate
		log.Infof("Consumption Pricesheet API: env var EURO_TO_USD_EXCHANGE_RATE not set: using rate of %v", fallbackRate)
	}
	return euro * exchangeRate
}

// Sanity check to see what pricing we got for LightOps in West Europe
// func (az *Azure) reportLightOpsVMPrices() {
// 	log.Info("*************************")
// 	log.Info("LightOps VM Pricing Info:")
// 	for k := range az.Pricing {
// 		if (strings.Contains(k, "NC8as_T4_v3") || strings.Contains(k, "D8s_v3") ||
// 			strings.Contains(k, "D16s_v3") || strings.Contains(k, "E16as_v5") ||
// 			strings.Contains(k, "D4as_v5") || strings.Contains(k, "E8as_v5") ||
// 			strings.Contains(k, "NV18ads") || strings.Contains(k, "NC24ads_A100_v4")) &&
// 			strings.Contains(k, "westeurope") && strings.Contains(k, "ondemand") &&
// 			!strings.Contains(k, "Promo") {
// 			log.Infof("    %v: hourly price %v", k, az.Pricing[k].Node.Cost)
// 		}
// 	}
// 	log.Info("*************************")
// }
