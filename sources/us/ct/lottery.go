// Copyright 2026 Neomantra Corp
//
// CT Weekly Cannabis Establishment Lottery Report
//
// Socrata Documentation:
//   https://dev.socrata.com/foundry/data.ct.gov/w85q-8cfm

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
	LotteryJSONFilename = "us_ct_lottery.json"
	LotteryCSVFilename  = "us_ct_lottery.csv"
	LotteryURL          = "https://data.ct.gov/resource/w85q-8cfm.json"
)

// Lottery represents a CT cannabis lottery report row
type Lottery struct {
	CredentialType             string `json:"credential_type"`
	SocialEquityCouncilLottery string `json:"social_equity_council_lottery"`
	GeneralLottery             string `json:"general_lottery"`
}

// SocialEquityCouncilLotteryInt returns the social equity lottery count as an integer
func (l Lottery) SocialEquityCouncilLotteryInt() int {
	n, _ := strconv.Atoi(l.SocialEquityCouncilLottery)
	return n
}

// GeneralLotteryInt returns the general lottery count as an integer
func (l Lottery) GeneralLotteryInt() int {
	n, _ := strconv.Atoi(l.GeneralLottery)
	return n
}

///////////////////////////////////////////////////////////////////////////////

// LotteryConfig returns the Socrata configuration for lottery
var LotteryConfig = sources.SocrataConfig{
	URL:           LotteryURL,
	CacheFilename: LotteryJSONFilename,
	OrderBy:       "credential_type",
}

// FetchLottery fetches all CT cannabis lottery data from the CT API
func FetchLottery(appToken string, maxCacheAge time.Duration) ([]Lottery, error) {
	return sources.FetchSocrata[Lottery](LotteryConfig, appToken, maxCacheAge)
}

///////////////////////////////////////////////////////////////////////////////

// CSVHeaders returns the CSV headers for the Lottery struct
func (l Lottery) CSVHeaders() string {
	return `"credential_type","social_equity_council_lottery","general_lottery"
`
}

// CSVValue returns the CSV value for the Lottery struct
func (l Lottery) CSVValue() string {
	return fmt.Sprintf(`"%s","%s","%s"
`, CSVString(l.CredentialType), l.SocialEquityCouncilLottery, l.GeneralLottery)
}

///////////////////////////////////////////////////////////////////////////////

// DBInsertLottery inserts lottery records into DuckDB
func DBInsertLottery(conn *sql.DB, lottery []Lottery) error {
	if len(lottery) == 0 {
		return nil
	}

	// Snapshot semantics: clear existing rows and reload
	if _, err := conn.Exec("DELETE FROM ct_lottery"); err != nil {
		return fmt.Errorf("failed to clear lottery: %w", err)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ct_lottery (credential_type, social_equity_council_lottery, general_lottery) VALUES ")

	for i, l := range lottery {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("('%s',%d,%d)",
			sources.SQLString(l.CredentialType),
			l.SocialEquityCouncilLotteryInt(),
			l.GeneralLotteryInt(),
		))
	}

	if _, err := conn.Exec(sb.String()); err != nil {
		return fmt.Errorf("failed to insert lottery: %w", err)
	}
	return nil
}
