// Copyright (c) 2025 Neomantra Corp

package sources

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// SocrataConfig holds configuration for a Socrata API endpoint
type SocrataConfig struct {
	URL           string // API endpoint URL
	CacheFilename string // Filename for caching results
	OrderBy       string // Field to order by (required for pagination)
	BatchSize     int    // Records per request (default 5000, set higher to disable pagination)
}

// FetchSocrata fetches data from a Socrata API endpoint with caching and pagination.
// It handles the common pattern of: check cache, paginate requests, unmarshal, cache.
func FetchSocrata[T any](cfg SocrataConfig, appToken string, maxCacheAge time.Duration) ([]T, error) {
	// Check cache first
	if cacheBytes, err := CheckCacheFile(cfg.CacheFilename, maxCacheAge); err == nil {
		var cached []T
		if err := json.Unmarshal(cacheBytes, &cached); err == nil {
			return cached, nil
		}
	}

	// Parse the base URL
	apiURL, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = 5000
	}

	client := &http.Client{}
	var allItems []T
	offset := 0

	// Paginate through results
	for {
		req, err := http.NewRequest("GET", apiURL.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Build query parameters
		q := req.URL.Query()
		q.Add("$limit", fmt.Sprintf("%d", batchSize))
		q.Add("$offset", fmt.Sprintf("%d", offset))
		if cfg.OrderBy != "" {
			q.Add("$order", cfg.OrderBy)
		}
		if appToken != "" {
			q.Add("$$app_token", appToken)
		}
		req.URL.RawQuery = q.Encode()

		// Make the request
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("HTTP request failed: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d %s %s", resp.StatusCode, resp.Status, string(body))
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		// Unmarshal batch
		var batch []T
		if err := json.Unmarshal(body, &batch); err != nil {
			return nil, fmt.Errorf("failed to unmarshal result: %w", err)
		}

		allItems = append(allItems, batch...)

		// Check if we've fetched all records
		if len(batch) < batchSize {
			break
		}
		offset += batchSize
	}

	// Cache the combined result
	if cacheFile, err := MakeCacheFile(cfg.CacheFilename); err == nil {
		if cacheBytes, err := json.Marshal(allItems); err == nil {
			cacheFile.Write(cacheBytes)
		}
		cacheFile.Close()
	}

	return allItems, nil
}
