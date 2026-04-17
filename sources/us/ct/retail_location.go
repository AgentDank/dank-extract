// Copyright 2026 Neomantra Corp
//
// CT Licensed Cannabis and Medical Marijuana Retail Locations
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/42yd-3x3d

package ct

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/AgentDank/dank-extract/sources"
)

const (
	RetailLocationJSONFilename = "us_ct_retail_locations.json"
	RetailLocationCSVFilename  = "us_ct_retail_locations.csv"
	RetailLocationsURL         = "https://data.ct.gov/resource/42yd-3x3d.json"
)

// RetailLocation represents a CT licensed cannabis/medical marijuana retail location
type RetailLocation struct {
	Type     string               `json:"type"`
	Business string               `json:"business"`
	DBA      string               `json:"dba"`
	License  string               `json:"license"`
	Street   string               `json:"street"`
	City     string               `json:"city"`
	Zipcode  string               `json:"zipcode"`
	Website  *RetailWebsite       `json:"website,omitempty"`
	Location *RetailLocationPoint `json:"location,omitempty"`
}

// RetailWebsite is the nested website object from the Socrata dataset
type RetailWebsite struct {
	URL string `json:"url"`
}

// RetailLocationPoint is a GeoJSON Point; Coordinates is [longitude, latitude]
type RetailLocationPoint struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

// WebsiteURL returns the website URL or empty string if absent
func (r RetailLocation) WebsiteURL() string {
	if r.Website == nil {
		return ""
	}
	return r.Website.URL
}

// Longitude returns the longitude or 0 if absent
func (r RetailLocation) Longitude() float64 {
	if r.Location == nil || len(r.Location.Coordinates) < 2 {
		return 0
	}
	return r.Location.Coordinates[0]
}

// Latitude returns the latitude or 0 if absent
func (r RetailLocation) Latitude() float64 {
	if r.Location == nil || len(r.Location.Coordinates) < 2 {
		return 0
	}
	return r.Location.Coordinates[1]
}

// HasLocation reports whether this record has valid coordinates
func (r RetailLocation) HasLocation() bool {
	return r.Location != nil && len(r.Location.Coordinates) >= 2
}

///////////////////////////////////////////////////////////////////////////////

// RetailLocationConfig returns the Socrata configuration for retail locations
var RetailLocationConfig = sources.SocrataConfig{
	URL:           RetailLocationsURL,
	CacheFilename: RetailLocationJSONFilename,
	OrderBy:       "license",
}

// FetchRetailLocations fetches all CT retail location data from the CT API
func FetchRetailLocations(appToken string, maxCacheAge time.Duration) ([]RetailLocation, error) {
	return sources.FetchSocrata[RetailLocation](RetailLocationConfig, appToken, maxCacheAge)
}

///////////////////////////////////////////////////////////////////////////////

// CSVHeaders returns the CSV headers for the RetailLocation struct
func (r RetailLocation) CSVHeaders() string {
	return `"type","business","dba","license","street","city","zipcode","website","longitude","latitude"
`
}

// CSVValue returns the CSV value for the RetailLocation struct
func (r RetailLocation) CSVValue() string {
	lon, lat := "", ""
	if r.HasLocation() {
		lon = fmt.Sprintf("%f", r.Longitude())
		lat = fmt.Sprintf("%f", r.Latitude())
	}
	return fmt.Sprintf(`"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"
`,
		CSVString(r.Type), CSVString(r.Business), CSVString(r.DBA), CSVString(r.License),
		CSVString(r.Street), CSVString(r.City), CSVString(r.Zipcode),
		CSVString(r.WebsiteURL()), lon, lat,
	)
}

///////////////////////////////////////////////////////////////////////////////

// DBInsertRetailLocations inserts retail locations into DuckDB
func DBInsertRetailLocations(conn *sql.DB, locations []RetailLocation) error {
	if len(locations) == 0 {
		return nil
	}

	// Snapshot semantics: clear existing rows and reload
	if _, err := conn.Exec("DELETE FROM ct_retail_locations"); err != nil {
		return fmt.Errorf("failed to clear retail locations: %w", err)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ct_retail_locations (type, business, dba, license, street, city, zipcode, website, longitude, latitude) VALUES ")

	for i, r := range locations {
		if i > 0 {
			sb.WriteString(",")
		}
		lonStr, latStr := "NULL", "NULL"
		if r.HasLocation() {
			lonStr = fmt.Sprintf("%f", r.Longitude())
			latStr = fmt.Sprintf("%f", r.Latitude())
		}
		sb.WriteString(fmt.Sprintf("('%s','%s','%s','%s','%s','%s','%s','%s',%s,%s)",
			sources.SQLString(r.Type),
			sources.SQLString(r.Business),
			sources.SQLString(r.DBA),
			sources.SQLString(r.License),
			sources.SQLString(r.Street),
			sources.SQLString(r.City),
			sources.SQLString(r.Zipcode),
			sources.SQLString(r.WebsiteURL()),
			lonStr,
			latStr,
		))
	}

	if _, err := conn.Exec(sb.String()); err != nil {
		return fmt.Errorf("failed to insert retail locations: %w", err)
	}
	return nil
}
