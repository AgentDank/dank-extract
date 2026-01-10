// Copyright 2026 Neomantra Corp
//
// CT Cannabis Credential Count Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/tjfe-s2x9

package ct

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/AgentDank/dank-extract/sources"
)

const (
	CredentialJSONFilename = "us_ct_credentials.json"
	CredentialCSVFilename  = "us_ct_credentials.csv"
	CredentialsURL         = "https://data.ct.gov/resource/tjfe-s2x9.json"
)

// Credential represents a CT cannabis credential count record
type Credential struct {
	CredentialType string `json:"credentialtype"`
	Status         string `json:"status"`
	Count          string `json:"count"`
}

// CountInt returns the count as an integer
func (c Credential) CountInt() int {
	n, _ := strconv.Atoi(c.Count)
	return n
}

///////////////////////////////////////////////////////////////////////////////

// CredentialConfig returns the Socrata configuration for credentials
var CredentialConfig = sources.SocrataConfig{
	URL:           CredentialsURL,
	CacheFilename: CredentialJSONFilename,
}

// FetchCredentials fetches all CT cannabis credential data from the CT API
func FetchCredentials(appToken string, maxCacheAge time.Duration) ([]Credential, error) {
	return sources.FetchSocrata[Credential](CredentialConfig, appToken, maxCacheAge)
}

///////////////////////////////////////////////////////////////////////////////

// CSVHeaders returns the CSV headers for the Credential struct
func (c Credential) CSVHeaders() string {
	return `"credential_type","status","count"
`
}

// CSVValue returns the CSV value for the Credential struct
func (c Credential) CSVValue() string {
	return fmt.Sprintf(`"%s","%s","%s"
`, CSVString(c.CredentialType), CSVString(c.Status), c.Count)
}

///////////////////////////////////////////////////////////////////////////////

// DBInsertCredentials inserts credentials into DuckDB
func DBInsertCredentials(conn *sql.DB, credentials []Credential) error {
	if len(credentials) == 0 {
		return nil
	}

	// Clear existing data and insert fresh (credentials are a snapshot, not append-only)
	if _, err := conn.Exec("DELETE FROM ct_credentials"); err != nil {
		return fmt.Errorf("failed to clear credentials: %w", err)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ct_credentials (credential_type, status, count) VALUES ")

	for i, c := range credentials {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("('%s','%s',%d)",
			sources.SQLString(c.CredentialType),
			sources.SQLString(c.Status),
			c.CountInt()))
	}

	if _, err := conn.Exec(sb.String()); err != nil {
		return fmt.Errorf("failed to insert credentials: %w", err)
	}
	return nil
}
