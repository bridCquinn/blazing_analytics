package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "github.com/lib/pq"

)

type App struct {
	DB *sql.DB
}

type DriveMetric struct {
	ID 			  int       `json:"id"`
	DATE          time.Time  `json:"date"`
	SERIAL_NUMBER string     `json:"serial_number"`
	MODEL        string      `json:"model"`
	CAPACITY_BYTES int64     `json:"capacity_bytes"`
	FAILURES      int        `json:"failures"`
	SMART_5_RAW  int64      `json:"smart_5_raw"`
	SMART_5_NORMALIZED int  `json:"smart_5_normalized"`
	SMART_187_RAW int64     `json:"smart_187_raw"`
	SMART_187_NORMALIZED int `json:"smart_187_normalized"`
	SMART_188_RAW int64     `json:"smart_188_raw"`
	SMART_188_NORMALIZED int `json:"smart_188_normalized"`
	SMART_197_RAW int64     `json:"smart_197_raw"`
	SMART_197_NORMALIZED int `json:"smart_197_normalized"`
	SMART_198_RAW int64     `json:"smart_198_raw"`
	SMART_198_NORMALIZED int `json:"smart_198_normalized"`
}

type FailureRateMetric struct {
	Model              string  `json:"model"`
	AverageFailureRate float64 `json:"average_failure_rate"`
	TotalSnapshots     int     `json:"total_snapshots"`
	TotalFailures      int     `json:"total_failures"`
}

func main() {
	connStr := "postgres://postgres:securepassword@localhost:5432/backblaze?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping the database: %v", err)
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(30 * time.Minute)

	app := &App{DB: db}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/drives", app.HandleDrivesCRUD)
	mux.HandleFunc("/api/metrics/failure-rates", app.GetFailureRates)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	log.Println("Server is running on port 8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}

}

func (a *App) HandleDrivesCRUD(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	switch r.Method {
	case http.MethodGet:
		idStr := r.URL.Query().Get("id")
		if idStr == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		id, err := strconv.Atoi(idStr)
		if err != nil {
			http.Error(w, "Invalid id parameter", http.StatusBadRequest)
			return
		}

		var d DriveMetric
		query := `SELECT id, date, serial_number, model, capacity_bytes, failure, smart_5_raw, smart_5_normalized, smart_187_raw, smart_187_normalized, smart_188_raw, smart_188_normalized, smart_197_raw, smart_197_normalized, smart_198_raw, smart_198_normalized FROM drive_metrics WHERE id = $1`
		err = a.DB.QueryRow(query, id).Scan(&d.ID, &d.DATE, &d.SERIAL_NUMBER, &d.MODEL, &d.CAPACITY_BYTES, &d.FAILURE,
			&d.SMART_5_RAW, &d.SMART_5_NORMALIZED,
			&d.SMART_187_RAW, &d.SMART_187_NORMALIZED,
			&d.SMART_188_RAW, &d.SMART_188_NORMALIZED,
			&d.SMART_197_RAW, &d.SMART_197_NORMALIZED,
			&d.SMART_198_RAW, &d.SMART_198_NORMALIZED)
		if err == sql.ErrNoRows {
			http.Error(w, "Drive metric not found", http.StatusNotFound)
			return
		} else if err != nil {
			http.Error(w, fmt.Sprintf("Failed to retrieve drive metric: %v", err), http.StatusInternalServerError)
			return
		}

		if err := json.NewEncoder(w).Encode(d); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}

	case http.MethodPost:
		var d DriveMetric
		if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
			http.Error(w, fmt.Sprintf("Failed to decode request body: %v", err), http.StatusBadRequest)
			return
		}
		query := `INSERT INTO drive_metrics (date, serial_number, model, capacity_bytes, failures, smart_5_raw, smart_5_normalized, smart_187_raw, smart_187_normalized, smart_188_raw, smart_188_normalized, smart_197_raw, smart_197_normalized, smart_198_raw, smart_198_normalized) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING id`
		err := a.DB.QueryRow(query, d.DATE, d.SERIAL_NUMBER, d.MODEL, d.CAPACITY_BYTES, d.FAILURES,
			d.SMART_5_RAW, d.SMART_5_NORMALIZED,
			d.SMART_187_RAW, d.SMART_187_NORMALIZED,
			d.SMART_188_RAW, d.SMART_188_NORMALIZED,
			d.SMART_197_RAW, d.SMART_197_NORMALIZED,
			d.SMART_198_RAW, d.SMART_198_NORMALIZED).Scan(&d.ID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to insert drive metric: %v", err), http.StatusInternalServerError)
			return
		}
		
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(d); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}

	case http.MethodDelete:
		idStr := r.URL.Query().Get("id")
		if idStr == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		id, err := strconv.Atoi(idStr)
		if err != nil {
			http.Error(w, "Invalid id parameter", http.StatusBadRequest)
			return
		}

		_, err = a.DB.Exec("DELETE FROM drive_metrics WHERE id = $1", id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete drive metric: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *App) GetFailureRates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")

	query := `Select model, average_failure_rate, total_snapshots, total_failures from mv_model_failure_rates order by average_failure_rate;`

	rows, err := a.DB.Query(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute query: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	metrics := make([]FailureRateMetric, 0)
	for rows.Next() {
		var m FailureRateMetric
		if err := rows.Scan(&m.Model, &m.AverageFailureRate, &m.TotalSnapshots, &m.TotalFailures); err != nil {
			http.Error(w, fmt.Sprintf("Failed to scan row: %v", err), http.StatusInternalServerError)
			return
		}
		metrics = append(metrics, m)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, fmt.Sprintf("Failed during rows iteration: %v", err), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}
}
