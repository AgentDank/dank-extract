// Copyright 2025 Neomantra Corp
//
// CT Cannabis Tax Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/jey2-vq68

package ct

import (
	"fmt"
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

// TaxConfig returns the Socrata configuration for tax data
var TaxConfig = sources.SocrataConfig{
	URL:           TaxURL,
	CacheFilename: TaxJSONFilename,
	OrderBy:       "period_end_date",
}

// FetchTax fetches all CT cannabis tax data from the CT API
func FetchTax(appToken string, maxCacheAge time.Duration) ([]Tax, error) {
	return sources.FetchSocrata[Tax](TaxConfig, appToken, maxCacheAge)
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
