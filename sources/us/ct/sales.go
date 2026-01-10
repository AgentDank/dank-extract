// Copyright 2025 Neomantra Corp
//
// CT Cannabis Retail Sales Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/ucaf-96h6

package ct

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/AgentDank/dank-extract/sources"
)

const (
	WeeklySalesJSONFilename = "us_ct_weekly_sales.json"
	WeeklySalesCSVFilename  = "us_ct_weekly_sales.csv"
	WeeklySalesURL          = "https://data.ct.gov/resource/ucaf-96h6.json"
)

// WeeklySales represents a CT cannabis weekly retail sales record
type WeeklySales struct {
	WeekEnding                       string `json:"unnamed_column"` // ISO 8601 datetime
	AdultUse                         string `json:"adult_use"`
	Medical                          string `json:"medical"`
	Total                            string `json:"total"`
	AdultUseProductsSold             string `json:"adult_use_products_sold"`
	MedicalProductsSold              string `json:"medical_products_sold"`
	TotalProductsSold                string `json:"total_products_sold"`
	AdultUseCannabisAveragePrice     string `json:"adult_use_cannabis_average_product_price"`
	MedicalMarijuanaAveragePrice     string `json:"medical_marijuana_average_product_price"`
}

///////////////////////////////////////////////////////////////////////////////

// FetchWeeklySales fetches all CT cannabis weekly sales data from the CT API
func FetchWeeklySales(appToken string, maxCacheAge time.Duration) ([]WeeklySales, error) {
	// check cache
	if cacheBytes, err := sources.CheckCacheFile(WeeklySalesJSONFilename, maxCacheAge); err == nil {
		var cached []WeeklySales
		if err := json.Unmarshal(cacheBytes, &cached); err == nil {
			return cached, nil
		}
	}

	// prepare the URL
	apiUrl, err := url.Parse(WeeklySalesURL)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", apiUrl.String(), nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("$limit", "50000")
	q.Add("$order", "unnamed_column")
	if appToken != "" {
		q.Add("$$app_token", appToken)
	}
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d %s %s", resp.StatusCode, resp.Status, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var sales []WeeklySales
	if err := json.Unmarshal(body, &sales); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	// cache the result
	if cacheFile, err := sources.MakeCacheFile(WeeklySalesJSONFilename); err == nil {
		cacheFile.Write(body)
		cacheFile.Close()
	}

	return sales, nil
}

///////////////////////////////////////////////////////////////////////////////

// CSVHeaders returns the CSV headers for the WeeklySales struct
func (s WeeklySales) CSVHeaders() string {
	return `"week_ending","adult_use","medical","total","adult_use_products_sold","medical_products_sold","total_products_sold","adult_use_avg_price","medical_avg_price"
`
}

// CSVValue returns the CSV value for the WeeklySales struct
func (s WeeklySales) CSVValue() string {
	return fmt.Sprintf(`"%s",%s,%s,%s,%s,%s,%s,%s,%s
`,
		s.WeekEnding,
		s.AdultUse,
		s.Medical,
		s.Total,
		s.AdultUseProductsSold,
		s.MedicalProductsSold,
		s.TotalProductsSold,
		s.AdultUseCannabisAveragePrice,
		s.MedicalMarijuanaAveragePrice,
	)
}

// WriteWeeklySalesCSV writes weekly sales to a CSV file
func WriteWeeklySalesCSV(filename string, sales []WeeklySales) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	file.WriteString(WeeklySales{}.CSVHeaders())
	for _, s := range sales {
		file.WriteString(s.CSVValue())
	}
	return nil
}

// WriteWeeklySalesJSON writes weekly sales to a JSON file
func WriteWeeklySalesJSON(filename string, sales []WeeklySales) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(sales)
}
