// Copyright 2026 Neomantra Corp
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
		appToken     string
		rootDir      string
		outputDir    string
		dbFile       string
		datasets     []string
		snapshotDir  string
		snapshotDate string
		noFetch      bool
		compress     bool
		verbose      bool
		showHelp     bool
		maxCacheAge  time.Duration
	)

	flag.StringVarP(&appToken, "token", "t", "", "ct.data.gov App Token")
	flag.StringVar(&rootDir, "root", ".", "Root directory for .dank data")
	flag.StringVarP(&outputDir, "output", "o", "", "Output directory for exports (default: current directory)")
	flag.StringVar(&dbFile, "db", "", "DuckDB file path (default: dank-data.duckdb)")
	flag.StringSliceVarP(&datasets, "dataset", "d", availableDatasets, "Datasets to fetch (brands,credentials,applications,sales,tax)")
	flag.StringVarP(&snapshotDir, "snapshot", "s", "", "Create snapshot in directory (e.g., ./snapshots)")
	flag.StringVar(&snapshotDate, "snapshot-date", "", "Snapshot date in YYYY-MM-DD format (default: today)")
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
		fmt.Println("Snapshot mode:")
		fmt.Println("  Use --snapshot to create a dated snapshot directory structure:")
		fmt.Println("  <snapshot-dir>/us/ct/YYYY-MM-DD/")
		fmt.Println()
		flag.PrintDefaults()
		os.Exit(0)
	}

	// Setup
	sources.SetDankRoot(rootDir)
	if err := sources.EnsureDankRoot(); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Handle snapshot mode
	if snapshotDir != "" {
		if snapshotDate == "" {
			snapshotDate = time.Now().Format("2006-01-02")
		}
		// Create snapshot directory structure: <snapshotDir>/us/ct/YYYY-MM-DD/
		outputDir = filepath.Join(snapshotDir, "us", "ct", snapshotDate)
		dbFile = filepath.Join(outputDir, "dank-data.duckdb")
		compress = true // Always compress in snapshot mode

		if err := os.MkdirAll(outputDir, 0755); err != nil {
			log.Fatalf("Failed to create snapshot directory: %v", err)
		}
		if verbose {
			log.Printf("Snapshot mode: output to %s", outputDir)
		}
	}

	if outputDir == "" {
		outputDir = "."
	}

	if dbFile == "" {
		dbFile = "dank-data.duckdb"
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

	if err := db.RunMigration(conn); err != nil {
		log.Fatalf("Failed to run migration: %v", err)
	}

	// Processing options passed to each processor
	opts := processOpts{
		appToken:    appToken,
		maxCacheAge: maxCacheAge,
		outputDir:   outputDir,
		conn:        conn,
		noFetch:     noFetch,
		compress:    compress,
		verbose:     verbose,
	}

	var outputFiles []string

	// Process each selected dataset
	processors := map[string]func(processOpts) ([]string, error){
		"brands":       processBrands,
		"credentials":  processCredentials,
		"applications": processApplications,
		"sales":        processWeeklySales,
		"tax":          processTax,
	}

	for _, name := range availableDatasets {
		if !datasetSet[name] {
			continue
		}
		processor := processors[name]
		files, err := processor(opts)
		if err != nil {
			log.Printf("Error processing %s: %v", name, err)
		} else {
			outputFiles = append(outputFiles, files...)
		}
	}

	// Close database connection before compressing (ensures all writes are flushed)
	if err := conn.Close(); err != nil {
		log.Fatalf("Failed to close DuckDB: %v", err)
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

// processOpts holds common options for all dataset processors
type processOpts struct {
	appToken    string
	maxCacheAge time.Duration
	outputDir   string
	conn        *sql.DB
	noFetch     bool
	compress    bool
	verbose     bool
}

// exportFiles writes data to CSV and JSON files, with optional compression.
// Returns the list of output files created.
func exportFiles[T sources.CSVExportable](data []T, csvFilename, jsonFilename string, opts processOpts) ([]string, error) {
	var files []string

	// Export to CSV
	csvFile := filepath.Join(opts.outputDir, csvFilename)
	if err := sources.WriteCSV(csvFile, data); err != nil {
		return nil, fmt.Errorf("failed to write CSV: %w", err)
	}
	if opts.compress {
		if err := compressFile(csvFile); err != nil {
			return nil, fmt.Errorf("failed to compress CSV: %w", err)
		}
		os.Remove(csvFile)
		files = append(files, csvFile+".zst")
	} else {
		files = append(files, csvFile)
	}

	// Export to JSON
	jsonFile := filepath.Join(opts.outputDir, jsonFilename)
	if err := sources.WriteJSON(jsonFile, data); err != nil {
		return nil, fmt.Errorf("failed to write JSON: %w", err)
	}
	if opts.compress {
		if err := compressFile(jsonFile); err != nil {
			return nil, fmt.Errorf("failed to compress JSON: %w", err)
		}
		os.Remove(jsonFile)
		files = append(files, jsonFile+".zst")
	} else {
		files = append(files, jsonFile)
	}

	return files, nil
}

// fetchOrLoadCache fetches data from API or loads from cache based on noFetch flag
func fetchOrLoadCache[T any](
	cacheFilename string,
	fetchFunc func(string, time.Duration) ([]T, error),
	opts processOpts,
) ([]T, error) {
	if opts.noFetch {
		cacheBytes, err := sources.CheckCacheFile(cacheFilename, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to load cache: %w", err)
		}
		var data []T
		if err := json.Unmarshal(cacheBytes, &data); err != nil {
			return nil, fmt.Errorf("failed to parse cached data: %w", err)
		}
		return data, nil
	}
	return fetchFunc(opts.appToken, opts.maxCacheAge)
}

func processBrands(opts processOpts) ([]string, error) {
	if opts.verbose {
		log.Println("Fetching CT brands data...")
	}

	brands, err := fetchOrLoadCache(ct.BrandJSONFilename, ct.FetchBrands, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch brands: %w", err)
	}
	if opts.verbose {
		log.Printf("Loaded %d brands", len(brands))
	}

	// Clean brands (specific to this dataset)
	originalCount := len(brands)
	brands = ct.CleanBrands(brands)
	if opts.verbose {
		log.Printf("Cleaned brands: %d -> %d (removed %d erroneous records)",
			originalCount, len(brands), originalCount-len(brands))
	}

	// Export files
	files, err := exportFiles(brands, ct.BrandCSVFilename, ct.BrandJSONFilename, opts)
	if err != nil {
		return nil, err
	}

	// Insert into DuckDB (specific to this dataset)
	if err := ct.DBInsertBrands(opts.conn, brands); err != nil {
		return nil, fmt.Errorf("failed to insert brands: %w", err)
	}

	if opts.verbose {
		log.Printf("Processed %d brands", len(brands))
	}

	return files, nil
}

func processCredentials(opts processOpts) ([]string, error) {
	if opts.verbose {
		log.Println("Fetching CT credentials data...")
	}

	credentials, err := fetchOrLoadCache(ct.CredentialJSONFilename, ct.FetchCredentials, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch credentials: %w", err)
	}
	if opts.verbose {
		log.Printf("Loaded %d credentials", len(credentials))
	}

	files, err := exportFiles(credentials, ct.CredentialCSVFilename, ct.CredentialJSONFilename, opts)
	if err != nil {
		return nil, err
	}

	if err := ct.DBInsertCredentials(opts.conn, credentials); err != nil {
		return nil, fmt.Errorf("failed to insert credentials: %w", err)
	}

	if opts.verbose {
		log.Printf("Processed %d credentials", len(credentials))
	}

	return files, nil
}

func processApplications(opts processOpts) ([]string, error) {
	if opts.verbose {
		log.Println("Fetching CT applications data...")
	}

	applications, err := fetchOrLoadCache(ct.ApplicationJSONFilename, ct.FetchApplications, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch applications: %w", err)
	}
	if opts.verbose {
		log.Printf("Loaded %d applications", len(applications))
	}

	files, err := exportFiles(applications, ct.ApplicationCSVFilename, ct.ApplicationJSONFilename, opts)
	if err != nil {
		return nil, err
	}

	if err := ct.DBInsertApplications(opts.conn, applications); err != nil {
		return nil, fmt.Errorf("failed to insert applications: %w", err)
	}

	if opts.verbose {
		log.Printf("Processed %d applications", len(applications))
	}

	return files, nil
}

func processWeeklySales(opts processOpts) ([]string, error) {
	if opts.verbose {
		log.Println("Fetching CT weekly sales data...")
	}

	sales, err := fetchOrLoadCache(ct.WeeklySalesJSONFilename, ct.FetchWeeklySales, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch weekly sales: %w", err)
	}
	if opts.verbose {
		log.Printf("Loaded %d weekly sales", len(sales))
	}

	files, err := exportFiles(sales, ct.WeeklySalesCSVFilename, ct.WeeklySalesJSONFilename, opts)
	if err != nil {
		return nil, err
	}

	if err := ct.DBInsertWeeklySales(opts.conn, sales); err != nil {
		return nil, fmt.Errorf("failed to insert weekly sales: %w", err)
	}

	if opts.verbose {
		log.Printf("Processed %d weekly sales", len(sales))
	}

	return files, nil
}

func processTax(opts processOpts) ([]string, error) {
	if opts.verbose {
		log.Println("Fetching CT tax data...")
	}

	taxes, err := fetchOrLoadCache(ct.TaxJSONFilename, ct.FetchTax, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tax: %w", err)
	}
	if opts.verbose {
		log.Printf("Loaded %d tax records", len(taxes))
	}

	files, err := exportFiles(taxes, ct.TaxCSVFilename, ct.TaxJSONFilename, opts)
	if err != nil {
		return nil, err
	}

	if err := ct.DBInsertTax(opts.conn, taxes); err != nil {
		return nil, fmt.Errorf("failed to insert tax: %w", err)
	}

	if opts.verbose {
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
