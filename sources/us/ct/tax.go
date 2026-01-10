// Copyright 2025 Neomantra Corp
//
// CT Cannabis Tax Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/jey2-vq68

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
	TaxJSONFilename = "us_ct_tax.json"
	TaxCSVFilename  = "us_ct_tax.csv"
	TaxURL          = "https://data.ct.gov/resource/jey2-vq68.json"
)

// Tax represents a CT cannabis monthly tax record
type Tax struct {
	PeriodEndDate     string `json:"period_end_date"` // ISO 8601 datetime
	Month             string `json:"month"`
	Year              string `json:"year"`
	FiscalYear        string `json:"fiscal_year"`
	PlantMaterialTax  string `json:"plant_material_tax"`
	EdibleProductsTax string `json:"edible_products_tax"`
	OtherCannabisTax  string `json:"other_cannabis_tax"`
	TotalTax          string `json:"total_tax"`
}

///////////////////////////////////////////////////////////////////////////////

// FetchTax fetches all CT cannabis tax data from the CT API
func FetchTax(appToken string, maxCacheAge time.Duration) ([]Tax, error) {
	// check cache
	if cacheBytes, err := sources.CheckCacheFile(TaxJSONFilename, maxCacheAge); err == nil {
		var cached []Tax
		if err := json.Unmarshal(cacheBytes, &cached); err == nil {
			return cached, nil
		}
	}

	// prepare the URL
	apiUrl, err := url.Parse(TaxURL)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", apiUrl.String(), nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("$limit", "50000")
	q.Add("$order", "period_end_date")
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

	var taxes []Tax
	if err := json.Unmarshal(body, &taxes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	// cache the result
	if cacheFile, err := sources.MakeCacheFile(TaxJSONFilename); err == nil {
		cacheFile.Write(body)
		cacheFile.Close()
	}

	return taxes, nil
}

///////////////////////////////////////////////////////////////////////////////

// CSVHeaders returns the CSV headers for the Tax struct
func (t Tax) CSVHeaders() string {
	return `"period_end_date","month","year","fiscal_year","plant_material_tax","edible_products_tax","other_cannabis_tax","total_tax"
`
}

// CSVValue returns the CSV value for the Tax struct
func (t Tax) CSVValue() string {
	return fmt.Sprintf(`"%s","%s","%s","%s",%s,%s,%s,%s
`,
		t.PeriodEndDate,
		t.Month,
		t.Year,
		t.FiscalYear,
		t.PlantMaterialTax,
		t.EdibleProductsTax,
		t.OtherCannabisTax,
		t.TotalTax,
	)
}

// WriteTaxCSV writes tax data to a CSV file
func WriteTaxCSV(filename string, taxes []Tax) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	file.WriteString(Tax{}.CSVHeaders())
	for _, t := range taxes {
		file.WriteString(t.CSVValue())
	}
	return nil
}

// WriteTaxJSON writes tax data to a JSON file
func WriteTaxJSON(filename string, taxes []Tax) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(taxes)
}
