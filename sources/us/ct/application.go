// Copyright 2025 Neomantra Corp
//
// CT Cannabis Applications Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/bqby-dyzr

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
	ApplicationJSONFilename = "us_ct_applications.json"
	ApplicationCSVFilename  = "us_ct_applications.csv"
	ApplicationsURL         = "https://data.ct.gov/resource/bqby-dyzr.json"
)

// ApplicationDocument represents a document attached to an application
type ApplicationDocument struct {
	URL string `json:"url"`
}

// Application represents a CT cannabis license application
type Application struct {
	ApplicationLicenseNumber    string              `json:"application_license_number"`
	ApplicationCredentialStatus string              `json:"application_credential_status"`
	StatusReason                string              `json:"status_reason"`
	SECReviewStatus             string              `json:"sec_review_status"`
	InitialApplicationType      string              `json:"initial_application_type"`
	HowSelected                 string              `json:"how_selected"`
	Name                        string              `json:"name"`
	Documents                   ApplicationDocument `json:"documents"`
}

///////////////////////////////////////////////////////////////////////////////

// FetchApplications fetches all CT cannabis application data from the CT API
func FetchApplications(appToken string, maxCacheAge time.Duration) ([]Application, error) {
	// check cache
	if cacheBytes, err := sources.CheckCacheFile(ApplicationJSONFilename, maxCacheAge); err == nil {
		var cached []Application
		if err := json.Unmarshal(cacheBytes, &cached); err == nil {
			return cached, nil
		}
	}

	// prepare the URL
	apiUrl, err := url.Parse(ApplicationsURL)
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

	var applications []Application
	if err := json.Unmarshal(body, &applications); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	// cache the result
	if cacheFile, err := sources.MakeCacheFile(ApplicationJSONFilename); err == nil {
		cacheFile.Write(body)
		cacheFile.Close()
	}

	return applications, nil
}

///////////////////////////////////////////////////////////////////////////////

// CSVHeaders returns the CSV headers for the Application struct
func (a Application) CSVHeaders() string {
	return `"application_license_number","application_credential_status","status_reason","sec_review_status","initial_application_type","how_selected","name","documents_url"
`
}

// CSVValue returns the CSV value for the Application struct
func (a Application) CSVValue() string {
	return fmt.Sprintf(`"%s","%s","%s","%s","%s","%s","%s","%s"
`,
		CSVString(a.ApplicationLicenseNumber),
		CSVString(a.ApplicationCredentialStatus),
		CSVString(a.StatusReason),
		CSVString(a.SECReviewStatus),
		CSVString(a.InitialApplicationType),
		CSVString(a.HowSelected),
		CSVString(a.Name),
		CSVString(a.Documents.URL),
	)
}

// WriteApplicationsCSV writes applications to a CSV file
func WriteApplicationsCSV(filename string, applications []Application) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	file.WriteString(Application{}.CSVHeaders())
	for _, a := range applications {
		file.WriteString(a.CSVValue())
	}
	return nil
}

// WriteApplicationsJSON writes applications to a JSON file
func WriteApplicationsJSON(filename string, applications []Application) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(applications)
}
