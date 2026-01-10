// Copyright 2025 Neomantra Corp
//
// dank-extract CLI - Cannabis data fetching, cleaning, and export tool

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AgentDank/dank-extract/internal/db"
	"github.com/AgentDank/dank-extract/sources"
	"github.com/AgentDank/dank-extract/sources/us/ct"
	"github.com/klauspost/compress/zstd"
	flag "github.com/spf13/pflag"
)

var availableDatasets = []string{
	"brands",
	"credentials",
	"applications",
	"sales",
	"tax",
}

func main() {
	// CLI flags
	var (
		appToken    string
		rootDir     string
		outputDir   string
		dbFile      string
		datasets    []string
		noFetch     bool
		compress    bool
		verbose     bool
		showHelp    bool
		maxCacheAge time.Duration
	)

	flag.StringVarP(&appToken, "token", "t", "", "ct.data.gov App Token")
	flag.StringVar(&rootDir, "root", ".", "Root directory for .dank data")
	flag.StringVarP(&outputDir, "output", "o", "", "Output directory for exports (default: current directory)")
	flag.StringVar(&dbFile, "db", "", "DuckDB file path (default: .dank/dank-extract.duckdb)")
	flag.StringSliceVarP(&datasets, "dataset", "d", availableDatasets, "Datasets to fetch (brands,credentials,applications,sales,tax)")
	flag.BoolVarP(&noFetch, "no-fetch", "n", false, "Don't fetch data, use existing cache")
	flag.BoolVarP(&compress, "compress", "c", false, "Compress output files with zstd")
	flag.BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	flag.DurationVar(&maxCacheAge, "max-cache-age", 24*time.Hour, "Maximum age of cached data before re-fetching")
	flag.BoolVarP(&showHelp, "help", "h", false, "Show help")

	flag.Parse()

	if showHelp {
		fmt.Println("dank-extract - Cannabis data fetching, cleaning, and export tool")
		fmt.Println()
		fmt.Println("Usage: dank-extract [options]")
		fmt.Println()
		fmt.Println("Available datasets: " + strings.Join(availableDatasets, ", "))
		fmt.Println()
		flag.PrintDefaults()
		os.Exit(0)
	}

	// Setup
	sources.SetDankRoot(rootDir)
	if err := sources.EnsureDankRoot(); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	if outputDir == "" {
		outputDir = "."
	}

	if dbFile == "" {
		dbFile = filepath.Join(sources.GetDankDir(), "dank-extract.duckdb")
	}

	// Convert datasets to a set for easy lookup
	datasetSet := make(map[string]bool)
	for _, d := range datasets {
		datasetSet[strings.ToLower(d)] = true
	}

	// Open DuckDB connection
	conn, err := sql.Open("duckdb", dbFile)
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer conn.Close()

	if err := db.RunMigration(conn); err != nil {
		log.Fatalf("Failed to run migration: %v", err)
	}

	var outputFiles []string

	// Process each dataset
	if datasetSet["brands"] {
		files, err := processBrands(appToken, maxCacheAge, outputDir, conn, noFetch, compress, verbose)
		if err != nil {
			log.Printf("Error processing brands: %v", err)
		} else {
			outputFiles = append(outputFiles, files...)
		}
	}

	if datasetSet["credentials"] {
		files, err := processCredentials(appToken, maxCacheAge, outputDir, noFetch, compress, verbose)
		if err != nil {
			log.Printf("Error processing credentials: %v", err)
		} else {
			outputFiles = append(outputFiles, files...)
		}
	}

	if datasetSet["applications"] {
		files, err := processApplications(appToken, maxCacheAge, outputDir, noFetch, compress, verbose)
		if err != nil {
			log.Printf("Error processing applications: %v", err)
		} else {
			outputFiles = append(outputFiles, files...)
		}
	}

	if datasetSet["sales"] {
		files, err := processWeeklySales(appToken, maxCacheAge, outputDir, noFetch, compress, verbose)
		if err != nil {
			log.Printf("Error processing sales: %v", err)
		} else {
			outputFiles = append(outputFiles, files...)
		}
	}

	if datasetSet["tax"] {
		files, err := processTax(appToken, maxCacheAge, outputDir, noFetch, compress, verbose)
		if err != nil {
			log.Printf("Error processing tax: %v", err)
		} else {
			outputFiles = append(outputFiles, files...)
		}
	}

	// Compress DuckDB if requested
	if compress {
		if err := compressFile(dbFile); err != nil {
			log.Fatalf("Failed to compress DuckDB: %v", err)
		}
		os.Remove(dbFile)
		outputFiles = append(outputFiles, dbFile+".zst")
		if verbose {
			log.Printf("Compressed DuckDB to %s.zst", dbFile)
		}
	} else {
		outputFiles = append(outputFiles, dbFile)
	}

	// Summary
	fmt.Println("Successfully processed CT cannabis datasets")
	fmt.Println("Output files:")
	for _, f := range outputFiles {
		fmt.Printf("  - %s\n", f)
	}
}

func processBrands(appToken string, maxCacheAge time.Duration, outputDir string, conn *sql.DB, noFetch, compress, verbose bool) ([]string, error) {
	if verbose {
		log.Println("Fetching CT brands data...")
	}

	var brands []ct.Brand
	var err error

	if noFetch {
		cacheBytes, err := sources.CheckCacheFile(ct.BrandJSONFilename, 0) // 0 = no age limit
		if err != nil {
			return nil, fmt.Errorf("failed to load cache: %w", err)
		}
		if err := json.Unmarshal(cacheBytes, &brands); err != nil {
			return nil, fmt.Errorf("failed to parse cached data: %w", err)
		}
		if verbose {
			log.Printf("Loaded %d brands from cache", len(brands))
		}
	} else {
		brands, err = ct.FetchBrands(appToken, maxCacheAge)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch brands: %w", err)
		}
		if verbose {
			log.Printf("Fetched %d brands from API", len(brands))
		}
	}

	// Clean brands
	originalCount := len(brands)
	brands = ct.CleanBrands(brands)
	if verbose {
		log.Printf("Cleaned brands: %d -> %d (removed %d erroneous records)",
			originalCount, len(brands), originalCount-len(brands))
	}

	var files []string

	// Export to CSV
	csvFile := filepath.Join(outputDir, ct.BrandCSVFilename)
	if err := ct.WriteBrandsCSV(csvFile, brands); err != nil {
		return nil, fmt.Errorf("failed to write CSV: %w", err)
	}
	if compress {
		if err := compressFile(csvFile); err != nil {
			return nil, fmt.Errorf("failed to compress CSV: %w", err)
		}
		os.Remove(csvFile)
		files = append(files, csvFile+".zst")
	} else {
		files = append(files, csvFile)
	}

	// Export to JSON
	jsonFile := filepath.Join(outputDir, ct.BrandJSONFilename)
	if err := ct.WriteBrandsJSON(jsonFile, brands); err != nil {
		return nil, fmt.Errorf("failed to write JSON: %w", err)
	}
	if compress {
		if err := compressFile(jsonFile); err != nil {
			return nil, fmt.Errorf("failed to compress JSON: %w", err)
		}
		os.Remove(jsonFile)
		files = append(files, jsonFile+".zst")
	} else {
		files = append(files, jsonFile)
	}

	// Insert into DuckDB
	if err := ct.DBInsertBrands(conn, brands); err != nil {
		return nil, fmt.Errorf("failed to insert brands: %w", err)
	}

	if verbose {
		log.Printf("Processed %d brands", len(brands))
	}

	return files, nil
}

func processCredentials(appToken string, maxCacheAge time.Duration, outputDir string, noFetch, compress, verbose bool) ([]string, error) {
	if verbose {
		log.Println("Fetching CT credentials data...")
	}

	var credentials []ct.Credential
	var err error

	if noFetch {
		cacheBytes, err := sources.CheckCacheFile(ct.CredentialJSONFilename, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to load cache: %w", err)
		}
		if err := json.Unmarshal(cacheBytes, &credentials); err != nil {
			return nil, fmt.Errorf("failed to parse cached data: %w", err)
		}
		if verbose {
			log.Printf("Loaded %d credentials from cache", len(credentials))
		}
	} else {
		credentials, err = ct.FetchCredentials(appToken, maxCacheAge)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch credentials: %w", err)
		}
		if verbose {
			log.Printf("Fetched %d credentials from API", len(credentials))
		}
	}

	var files []string

	// Export to CSV
	csvFile := filepath.Join(outputDir, ct.CredentialCSVFilename)
	if err := ct.WriteCredentialsCSV(csvFile, credentials); err != nil {
		return nil, fmt.Errorf("failed to write CSV: %w", err)
	}
	if compress {
		if err := compressFile(csvFile); err != nil {
			return nil, fmt.Errorf("failed to compress CSV: %w", err)
		}
		os.Remove(csvFile)
		files = append(files, csvFile+".zst")
	} else {
		files = append(files, csvFile)
	}

	// Export to JSON
	jsonFile := filepath.Join(outputDir, ct.CredentialJSONFilename)
	if err := ct.WriteCredentialsJSON(jsonFile, credentials); err != nil {
		return nil, fmt.Errorf("failed to write JSON: %w", err)
	}
	if compress {
		if err := compressFile(jsonFile); err != nil {
			return nil, fmt.Errorf("failed to compress JSON: %w", err)
		}
		os.Remove(jsonFile)
		files = append(files, jsonFile+".zst")
	} else {
		files = append(files, jsonFile)
	}

	if verbose {
		log.Printf("Processed %d credentials", len(credentials))
	}

	return files, nil
}

func processApplications(appToken string, maxCacheAge time.Duration, outputDir string, noFetch, compress, verbose bool) ([]string, error) {
	if verbose {
		log.Println("Fetching CT applications data...")
	}

	var applications []ct.Application
	var err error

	if noFetch {
		cacheBytes, err := sources.CheckCacheFile(ct.ApplicationJSONFilename, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to load cache: %w", err)
		}
		if err := json.Unmarshal(cacheBytes, &applications); err != nil {
			return nil, fmt.Errorf("failed to parse cached data: %w", err)
		}
		if verbose {
			log.Printf("Loaded %d applications from cache", len(applications))
		}
	} else {
		applications, err = ct.FetchApplications(appToken, maxCacheAge)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch applications: %w", err)
		}
		if verbose {
			log.Printf("Fetched %d applications from API", len(applications))
		}
	}

	var files []string

	// Export to CSV
	csvFile := filepath.Join(outputDir, ct.ApplicationCSVFilename)
	if err := ct.WriteApplicationsCSV(csvFile, applications); err != nil {
		return nil, fmt.Errorf("failed to write CSV: %w", err)
	}
	if compress {
		if err := compressFile(csvFile); err != nil {
			return nil, fmt.Errorf("failed to compress CSV: %w", err)
		}
		os.Remove(csvFile)
		files = append(files, csvFile+".zst")
	} else {
		files = append(files, csvFile)
	}

	// Export to JSON
	jsonFile := filepath.Join(outputDir, ct.ApplicationJSONFilename)
	if err := ct.WriteApplicationsJSON(jsonFile, applications); err != nil {
		return nil, fmt.Errorf("failed to write JSON: %w", err)
	}
	if compress {
		if err := compressFile(jsonFile); err != nil {
			return nil, fmt.Errorf("failed to compress JSON: %w", err)
		}
		os.Remove(jsonFile)
		files = append(files, jsonFile+".zst")
	} else {
		files = append(files, jsonFile)
	}

	if verbose {
		log.Printf("Processed %d applications", len(applications))
	}

	return files, nil
}

func processWeeklySales(appToken string, maxCacheAge time.Duration, outputDir string, noFetch, compress, verbose bool) ([]string, error) {
	if verbose {
		log.Println("Fetching CT weekly sales data...")
	}

	var sales []ct.WeeklySales
	var err error

	if noFetch {
		cacheBytes, err := sources.CheckCacheFile(ct.WeeklySalesJSONFilename, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to load cache: %w", err)
		}
		if err := json.Unmarshal(cacheBytes, &sales); err != nil {
			return nil, fmt.Errorf("failed to parse cached data: %w", err)
		}
		if verbose {
			log.Printf("Loaded %d weekly sales from cache", len(sales))
		}
	} else {
		sales, err = ct.FetchWeeklySales(appToken, maxCacheAge)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch weekly sales: %w", err)
		}
		if verbose {
			log.Printf("Fetched %d weekly sales from API", len(sales))
		}
	}

	var files []string

	// Export to CSV
	csvFile := filepath.Join(outputDir, ct.WeeklySalesCSVFilename)
	if err := ct.WriteWeeklySalesCSV(csvFile, sales); err != nil {
		return nil, fmt.Errorf("failed to write CSV: %w", err)
	}
	if compress {
		if err := compressFile(csvFile); err != nil {
			return nil, fmt.Errorf("failed to compress CSV: %w", err)
		}
		os.Remove(csvFile)
		files = append(files, csvFile+".zst")
	} else {
		files = append(files, csvFile)
	}

	// Export to JSON
	jsonFile := filepath.Join(outputDir, ct.WeeklySalesJSONFilename)
	if err := ct.WriteWeeklySalesJSON(jsonFile, sales); err != nil {
		return nil, fmt.Errorf("failed to write JSON: %w", err)
	}
	if compress {
		if err := compressFile(jsonFile); err != nil {
			return nil, fmt.Errorf("failed to compress JSON: %w", err)
		}
		os.Remove(jsonFile)
		files = append(files, jsonFile+".zst")
	} else {
		files = append(files, jsonFile)
	}

	if verbose {
		log.Printf("Processed %d weekly sales", len(sales))
	}

	return files, nil
}

func processTax(appToken string, maxCacheAge time.Duration, outputDir string, noFetch, compress, verbose bool) ([]string, error) {
	if verbose {
		log.Println("Fetching CT tax data...")
	}

	var taxes []ct.Tax
	var err error

	if noFetch {
		cacheBytes, err := sources.CheckCacheFile(ct.TaxJSONFilename, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to load cache: %w", err)
		}
		if err := json.Unmarshal(cacheBytes, &taxes); err != nil {
			return nil, fmt.Errorf("failed to parse cached data: %w", err)
		}
		if verbose {
			log.Printf("Loaded %d tax records from cache", len(taxes))
		}
	} else {
		taxes, err = ct.FetchTax(appToken, maxCacheAge)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch tax: %w", err)
		}
		if verbose {
			log.Printf("Fetched %d tax records from API", len(taxes))
		}
	}

	var files []string

	// Export to CSV
	csvFile := filepath.Join(outputDir, ct.TaxCSVFilename)
	if err := ct.WriteTaxCSV(csvFile, taxes); err != nil {
		return nil, fmt.Errorf("failed to write CSV: %w", err)
	}
	if compress {
		if err := compressFile(csvFile); err != nil {
			return nil, fmt.Errorf("failed to compress CSV: %w", err)
		}
		os.Remove(csvFile)
		files = append(files, csvFile+".zst")
	} else {
		files = append(files, csvFile)
	}

	// Export to JSON
	jsonFile := filepath.Join(outputDir, ct.TaxJSONFilename)
	if err := ct.WriteTaxJSON(jsonFile, taxes); err != nil {
		return nil, fmt.Errorf("failed to write JSON: %w", err)
	}
	if compress {
		if err := compressFile(jsonFile); err != nil {
			return nil, fmt.Errorf("failed to compress JSON: %w", err)
		}
		os.Remove(jsonFile)
		files = append(files, jsonFile+".zst")
	} else {
		files = append(files, jsonFile)
	}

	if verbose {
		log.Printf("Processed %d tax records", len(taxes))
	}

	return files, nil
}

func compressFile(filename string) error {
	input, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file for compression: %w", err)
	}

	output, err := os.Create(filename + ".zst")
	if err != nil {
		return fmt.Errorf("failed to create compressed file: %w", err)
	}
	defer output.Close()

	encoder, err := zstd.NewWriter(output)
	if err != nil {
		return fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	defer encoder.Close()

	_, err = encoder.Write(input)
	if err != nil {
		return fmt.Errorf("failed to write compressed data: %w", err)
	}

	return nil
}
