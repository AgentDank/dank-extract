// Copyright 2025 Neomantra Corp
//
// CT Cannabis Credential Count Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/tjfe-s2x9

package ct

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
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

// FetchCredentials fetches all CT cannabis credential data from the CT API
func FetchCredentials(appToken string, maxCacheAge time.Duration) ([]Credential, error) {
	// check cache
	if cacheBytes, err := sources.CheckCacheFile(CredentialJSONFilename, maxCacheAge); err == nil {
		var cached []Credential
		if err := json.Unmarshal(cacheBytes, &cached); err == nil {
			return cached, nil
		}
	}

	// prepare the URL
	apiUrl, err := url.Parse(CredentialsURL)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", apiUrl.String(), nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("$limit", "50000")
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

	var credentials []Credential
	if err := json.Unmarshal(body, &credentials); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	// cache the result
	if cacheFile, err := sources.MakeCacheFile(CredentialJSONFilename); err == nil {
		cacheFile.Write(body)
		cacheFile.Close()
	}

	return credentials, nil
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

// WriteCredentialsCSV writes credentials to a CSV file
func WriteCredentialsCSV(filename string, credentials []Credential) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	file.WriteString(Credential{}.CSVHeaders())
	for _, c := range credentials {
		file.WriteString(c.CSVValue())
	}
	return nil
}

// WriteCredentialsJSON writes credentials to a JSON file
func WriteCredentialsJSON(filename string, credentials []Credential) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(credentials)
}
