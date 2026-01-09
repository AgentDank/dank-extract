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
	"time"

	"github.com/AgentDank/dank-extract/internal/db"
	"github.com/AgentDank/dank-extract/sources"
	"github.com/AgentDank/dank-extract/sources/us/ct"
	"github.com/klauspost/compress/zstd"
	flag "github.com/spf13/pflag"
)

func main() {
	// CLI flags
	var (
		appToken    string
		rootDir     string
		outputDir   string
		dbFile      string
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

	// Fetch brands
	if verbose {
		log.Println("Fetching CT brands data...")
	}

	var brands []ct.Brand
	var err error

	if noFetch {
		// Load from cache only
		cacheBytes, err := sources.CheckCacheFile(ct.BrandJSONFilename, 0) // 0 = no age limit
		if err != nil {
			log.Fatalf("Failed to load cache (use without --no-fetch to fetch fresh data): %v", err)
		}
		if err := json.Unmarshal(cacheBytes, &brands); err != nil {
			log.Fatalf("Failed to parse cached data: %v", err)
		}
		if verbose {
			log.Printf("Loaded %d brands from cache", len(brands))
		}
	} else {
		brands, err = ct.FetchBrands(appToken, maxCacheAge)
		if err != nil {
			log.Fatalf("Failed to fetch brands: %v", err)
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

	// Export to CSV
	csvFile := filepath.Join(outputDir, ct.BrandCSVFilename)
	if err := ct.WriteBrandsCSV(csvFile, brands); err != nil {
		log.Fatalf("Failed to write CSV: %v", err)
	}
	if verbose {
		log.Printf("Wrote CSV to %s", csvFile)
	}

	// Compress if requested
	if compress {
		if err := compressFile(csvFile); err != nil {
			log.Fatalf("Failed to compress CSV: %v", err)
		}
		os.Remove(csvFile) // Remove uncompressed file
		if verbose {
			log.Printf("Compressed CSV to %s.zst", csvFile)
		}
	}

	// Export to JSON
	jsonFile := filepath.Join(outputDir, ct.BrandJSONFilename)
	if err := ct.WriteBrandsJSON(jsonFile, brands); err != nil {
		log.Fatalf("Failed to write JSON: %v", err)
	}
	if verbose {
		log.Printf("Wrote JSON to %s", jsonFile)
	}

	if compress {
		if err := compressFile(jsonFile); err != nil {
			log.Fatalf("Failed to compress JSON: %v", err)
		}
		os.Remove(jsonFile)
		if verbose {
			log.Printf("Compressed JSON to %s.zst", jsonFile)
		}
	}

	// Export to DuckDB
	conn, err := sql.Open("duckdb", dbFile)
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer conn.Close()

	if err := db.RunMigration(conn); err != nil {
		log.Fatalf("Failed to run migration: %v", err)
	}

	if err := ct.DBInsertBrands(conn, brands); err != nil {
		log.Fatalf("Failed to insert brands: %v", err)
	}
	if verbose {
		log.Printf("Wrote DuckDB to %s", dbFile)
	}

	if compress {
		if err := compressFile(dbFile); err != nil {
			log.Fatalf("Failed to compress DuckDB: %v", err)
		}
		os.Remove(dbFile)
		if verbose {
			log.Printf("Compressed DuckDB to %s.zst", dbFile)
		}
	}

	// Summary
	fmt.Printf("Successfully processed %d CT cannabis brands\n", len(brands))
	fmt.Printf("Output files:\n")
	if compress {
		fmt.Printf("  - %s.zst\n", csvFile)
		fmt.Printf("  - %s.zst\n", jsonFile)
		fmt.Printf("  - %s.zst\n", dbFile)
	} else {
		fmt.Printf("  - %s\n", csvFile)
		fmt.Printf("  - %s\n", jsonFile)
		fmt.Printf("  - %s\n", dbFile)
	}
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
