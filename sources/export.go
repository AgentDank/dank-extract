// Copyright (c) 2025 Neomantra Corp

package sources

import (
	"encoding/json"
	"fmt"
	"os"
)

// CSVExportable is an interface for types that can be exported to CSV
type CSVExportable interface {
	CSVHeaders() string
	CSVValue() string
}

// WriteJSON writes any slice of items to a JSON file with pretty formatting
func WriteJSON[T any](filename string, items []T) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(items)
}

// WriteCSV writes any slice of CSVExportable items to a CSV file
func WriteCSV[T CSVExportable](filename string, items []T) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	if len(items) > 0 {
		file.WriteString(items[0].CSVHeaders())
	}
	for _, item := range items {
		file.WriteString(item.CSVValue())
	}
	return nil
}
