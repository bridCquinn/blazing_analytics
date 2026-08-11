package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DriveMetricRow struct {
	DATE time.Time `json:"date"`
	SERIAL_NUMBER string `json:"serial_number"`
	MODEL string `json:"model"`
	CAPACITY_BYTES int64 `json:"capacity_bytes"`
	FAILURE int `json:"failure"`
	SMART_5_RAW int64 `json:"smart_5_raw"`
	SMART_5_NORMALIZED int `json:"smart_5_normalized"`
	SMART_187_RAW int64 `json:"smart_187_raw"`
	SMART_187_NORMALIZED int `json:"smart_187_normalized"`
	SMART_188_RAW int64 `json:"smart_188_raw"`
	SMART_188_NORMALIZED int `json:"smart_188_normalized"`
	SMART_197_RAW int64 `json:"smart_197_raw"`
	SMART_197_NORMALIZED int `json:"smart_197_normalized"`
	SMART_198_RAW int64 `json:"smart_198_raw"`
	SMART_198_NORMALIZED int `json:"smart_198_normalized"`
}

func main() {
	start := time.Now()
	connStr := "postgres://postgres:securepassword@localhost:5432/backblaze?sslmode=disable&pool_max_conns=10"
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Database connection breakdown: %v", err)
	}
	defer pool.Close()

	basePath := "/Users/bridgetwoodye/Projects/csv_files"
	folders := []string{"data_Q1_2026", "data_Q4_2025"}

	var allFiles []string
	for _, folder := range folders {
		matches, _ := filepath.Glob(filepath.Join(basePath, folder, "*.csv"))
		allFiles = append(allFiles, matches...)
	}

	log.Printf("Found %d CSV files to process.", len(allFiles))

	fileChan := make(chan string, len(allFiles))
	for _, file := range allFiles {
		fileChan <- file
	}
	close(fileChan)

	var wg sync.WaitGroup
	numWorkers := 4

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filePath := range fileChan {
				if err := processFileWithCopyFrom(ctx, pool, filePath); err != nil {
					log.Printf("Error processing file %s: %v", filePath, err)
				}
			}
		}()
	}

	wg.Wait()
	log.Printf("Refreshing materialized view...")
	_, err = pool.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_model_failure_rates;")
	if err != nil {
		log.Printf("Error refreshing materialized view: %v", err)
	}

	log.Printf("All files processed and materialized view refreshed in %v.", time.Since(start))
}

func processFileWithCopyFrom(ctx context.Context, pool *pgxpool.Pool, file string) error {
	// Open the CSV file
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("Failed to open file: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("Failed to read headers: %w", err)
	}

	colIndices := map[string]int{
		"date":                  -1,
		"serial_number":         -1,
		"model":                 -1,
		"capacity_bytes":        -1,
		"failure":              -1,
		"smart_5_raw":          -1,
		"smart_5_normalized":   -1,
		"smart_187_raw":       -1,
		"smart_187_normalized": -1,
		"smart_188_raw":       -1,
		"smart_188_normalized": -1,
		"smart_197_raw":      -1,
		"smart_197_normalized": -1,
		"smart_198_raw":      -1,
		"smart_198_normalized": -1,
	}

	for i, col := range headers {
		cleanCol := strings.TrimSpace(strings.ToLower(col))
		if _, exists := colIndices[cleanCol]; exists {
			colIndices[cleanCol] = i
		}
	}

	// Stream parsing rows into memeory block chunks for CopyFrom
	var rows [][]any
	totalStreamed := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // Skip malformed rows
		}

		dateVal := parseDate(getVal (record, colIndices["date"]))
		serialVal := getVal(record, colIndices["serial_number"])
		modelVal := getVal(record, colIndices["model"])

		if dateVal.IsZero() || serialVal == "" || modelVal == "" {
			continue // Skip rows with essential missing data
		}

		rows = append(rows, []any{
			dateVal,
			serialVal,
			modelVal,
			parseInt64(getVal(record, colIndices["capacity_bytes"])),
			parseInt(getVal(record, colIndices["failure"])),
			parseInt64(getVal(record, colIndices["smart_5_raw"])),
			parseIntPtr(getVal(record, colIndices["smart_5_normalized"])),
			parseInt64(getVal(record, colIndices["smart_187_raw"])),
			parseIntPtr(getVal(record, colIndices["smart_187_normalized"])),
			parseInt64(getVal(record, colIndices["smart_188_raw"])),
			parseIntPtr(getVal(record, colIndices["smart_188_normalized"])),
			parseInt64(getVal(record, colIndices["smart_197_raw"])),
			parseIntPtr(getVal(record, colIndices["smart_197_normalized"])),
			parseInt64(getVal(record, colIndices["smart_198_raw"])),
			parseIntPtr(getVal(record, colIndices["smart_198_normalized"])),
		})

		// Flush out via CopyFrom protocol in chunks
		if len(rows) >= 10000 {
			err = writeCopy(ctx, pool, rows)
			if err != nil {
				return fmt.Errorf("Failed to write chunk: %w", err)
			}
			totalStreamed += len(rows)
			rows = rows[:0] // Reset slice for next chunk
		}
	}

	if len(rows) > 0 {
		err = writeCopy(ctx, pool, rows)
		if err != nil {
			return fmt.Errorf("Failed to write final chunk: %w", err)
		}
		totalStreamed += len(rows)
	}
	
	log.Printf("Successfully processed %d rows from file %s", totalStreamed, file)
	return nil
}

func writeCopy(ctx context.Context, pool *pgxpool.Pool, rows [][]any) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("Failed to acquire connection: %w", err)
	}
	defer conn.Release()

	tableName := pgx.Identifier{"hard_drive_metrics"}
	columnNames := []string{
		"date",
		"serial_number",
		"model",
		"capacity_bytes",
		"failure",
		"smart_5_raw",
		"smart_5_normalized",
		"smart_187_raw",
		"smart_187_normalized",
		"smart_188_raw",
		"smart_188_normalized",
		"smart_197_raw",
		"smart_197_normalized",
		"smart_198_raw",
		"smart_198_normalized",
	}

	_, err = conn.Conn().CopyFrom(
		ctx,
		tableName,
		columnNames,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("Failed to copy rows: %w", err)
	}

	return nil
}

func parseDate(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func parseInt64(s string) *int64 {
	if s == "" {
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}

func parseInt(s string) int {
	if s == "" {
		return 0
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return v
}

func parseIntPtr(s string) *int {
	if s == "" {
		return nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return nil
	}
	return &v
}

func getVal(record []string, idx int) string {
	if idx == -1 || idx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[idx])
}
