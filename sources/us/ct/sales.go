// Copyright 2025 Neomantra Corp
//
// CT Cannabis Retail Sales Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/ucaf-96h6

package ct

import (
	"database/sql"
	"fmt"
	"strings"
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

// WeeklySalesConfig returns the Socrata configuration for weekly sales
var WeeklySalesConfig = sources.SocrataConfig{
	URL:           WeeklySalesURL,
	CacheFilename: WeeklySalesJSONFilename,
	OrderBy:       "unnamed_column",
}

// FetchWeeklySales fetches all CT cannabis weekly sales data from the CT API
func FetchWeeklySales(appToken string, maxCacheAge time.Duration) ([]WeeklySales, error) {
	return sources.FetchSocrata[WeeklySales](WeeklySalesConfig, appToken, maxCacheAge)
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

///////////////////////////////////////////////////////////////////////////////

// sqlNum converts a string number to SQL value, returning NULL for empty
func sqlNum(s string) string {
	if s == "" {
		return "NULL"
	}
	return s
}

// DBInsertWeeklySales inserts weekly sales into DuckDB
func DBInsertWeeklySales(conn *sql.DB, sales []WeeklySales) error {
	if len(sales) == 0 {
		return nil
	}

	// Clear existing data and insert fresh
	if _, err := conn.Exec("DELETE FROM ct_weekly_sales"); err != nil {
		return fmt.Errorf("failed to clear weekly sales: %w", err)
	}

	var sb strings.Builder
	sb.WriteString(`INSERT INTO ct_weekly_sales (
		week_ending, adult_use, medical, total,
		adult_use_products_sold, medical_products_sold, total_products_sold,
		adult_use_avg_price, medical_avg_price
	) VALUES `)

	for i, s := range sales {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("('%s',%s,%s,%s,%s,%s,%s,%s,%s)",
			sources.SQLString(s.WeekEnding),
			sqlNum(s.AdultUse),
			sqlNum(s.Medical),
			sqlNum(s.Total),
			sqlNum(s.AdultUseProductsSold),
			sqlNum(s.MedicalProductsSold),
			sqlNum(s.TotalProductsSold),
			sqlNum(s.AdultUseCannabisAveragePrice),
			sqlNum(s.MedicalMarijuanaAveragePrice)))
	}

	if _, err := conn.Exec(sb.String()); err != nil {
		return fmt.Errorf("failed to insert weekly sales: %w", err)
	}
	return nil
}

