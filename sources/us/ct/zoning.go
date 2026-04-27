// Copyright 2026 Neomantra Corp
//
// CT Cannabis Zoning Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/khc7-gd9u

package ct

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/AgentDank/dank-extract/sources"
)

const (
	ZoningJSONFilename = "us_ct_zoning.json"
	ZoningCSVFilename  = "us_ct_zoning.csv"
	ZoningURL          = "https://data.ct.gov/resource/khc7-gd9u.json"
)

// Zoning represents a CT cannabis zoning record for a municipality
type Zoning struct {
	Town           string `json:"town"`
	State          string `json:"state"`
	Status         string `json:"status"`
	DateAdopted    string `json:"date_adopted"`
	DateEffective  string `json:"date_effective"`
	Notes          string `json:"notes"`
	DateAdopted1   string `json:"date_adopted_1"`
	DateEffective1 string `json:"date_effective_1"`
	Notes1         string `json:"notes_1"`
}

///////////////////////////////////////////////////////////////////////////////

// ZoningConfig returns the Socrata configuration for zoning
var ZoningConfig = sources.SocrataConfig{
	URL:           ZoningURL,
	CacheFilename: ZoningJSONFilename,
	OrderBy:       "town",
}

// FetchZoning fetches all CT cannabis zoning data from the CT API
func FetchZoning(appToken string, maxCacheAge time.Duration) ([]Zoning, error) {
	return sources.FetchSocrata[Zoning](ZoningConfig, appToken, maxCacheAge)
}

///////////////////////////////////////////////////////////////////////////////

// CSVHeaders returns the CSV headers for the Zoning struct
func (z Zoning) CSVHeaders() string {
	return `"town","state","status","date_adopted","date_effective","notes","date_adopted_1","date_effective_1","notes_1"
`
}

// CSVValue returns the CSV value for the Zoning struct
func (z Zoning) CSVValue() string {
	return fmt.Sprintf(`"%s","%s","%s","%s","%s","%s","%s","%s","%s"
`,
		CSVString(z.Town), CSVString(z.State), CSVString(z.Status),
		CSVString(z.DateAdopted), CSVString(z.DateEffective), CSVString(z.Notes),
		CSVString(z.DateAdopted1), CSVString(z.DateEffective1), CSVString(z.Notes1))
}

///////////////////////////////////////////////////////////////////////////////

// DBInsertZoning inserts zoning records into DuckDB
func DBInsertZoning(conn *sql.DB, zoning []Zoning) error {
	if len(zoning) == 0 {
		return nil
	}

	// Snapshot semantics: clear existing rows and reload
	if _, err := conn.Exec("DELETE FROM ct_zoning"); err != nil {
		return fmt.Errorf("failed to clear zoning: %w", err)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ct_zoning (town, state, status, date_adopted, date_effective, notes, date_adopted_1, date_effective_1, notes_1) VALUES ")

	for i, z := range zoning {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("('%s','%s','%s','%s','%s','%s','%s','%s','%s')",
			sources.SQLString(z.Town),
			sources.SQLString(z.State),
			sources.SQLString(z.Status),
			sources.SQLString(z.DateAdopted),
			sources.SQLString(z.DateEffective),
			sources.SQLString(z.Notes),
			sources.SQLString(z.DateAdopted1),
			sources.SQLString(z.DateEffective1),
			sources.SQLString(z.Notes1),
		))
	}

	if _, err := conn.Exec(sb.String()); err != nil {
		return fmt.Errorf("failed to insert zoning: %w", err)
	}
	return nil
}
