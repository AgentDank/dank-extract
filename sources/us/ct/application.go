// Copyright 2025 Neomantra Corp
//
// CT Cannabis Applications Data
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/bqby-dyzr

package ct

import (
	"fmt"
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

// ApplicationConfig returns the Socrata configuration for applications
var ApplicationConfig = sources.SocrataConfig{
	URL:           ApplicationsURL,
	CacheFilename: ApplicationJSONFilename,
}

// FetchApplications fetches all CT cannabis application data from the CT API
func FetchApplications(appToken string, maxCacheAge time.Duration) ([]Application, error) {
	return sources.FetchSocrata[Application](ApplicationConfig, appToken, maxCacheAge)
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

