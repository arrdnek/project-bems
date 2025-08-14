package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

var db *sql.DB
var mqttClient mqtt.Client

const (
	localDSN = "root:xxx@tcp(database:3306)/xxx?parseTime=true"
)

func initDB() *sql.DB {
	localDB, err := sql.Open("mysql", localDSN)
	if err != nil {
		log.Fatalf("Gagal koneksi ke database lokal: %v", err)
	}
	log.Println("Berhasil koneksi ke basis data")
	return localDB
}

/* KODE PROGRAM - INISIASI KONEKSI */
func initMQTT() {
	opts := mqtt.NewClientOptions().
		AddBroker("mqtt://emqx-lb:1883").
		SetUsername("heb_iot").
		SetPassword("y4kin94n?")

	mqttClient = mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to the MQTT broker: %v", token.Error())
	}
	log.Println("Berhasil membuat koneksi ke broker MQTT")

	if token := mqttClient.Subscribe("monitoring/sensor", 0, receivedMessageHandler); token.Wait() && token.Error() != nil {
		log.Fatalf("Error subscribing to MQTT topic: %v", token.Error())
	} else {
		log.Println("Berhasil subscribe topik 'monitoring/sensor'")
	}

}

/* KODE PROGRAM - PENERIMAAN PESAN */
func receivedMessageHandler(client mqtt.Client, msg mqtt.Message) {
	go func(m mqtt.Message) {
		startTime := time.Now()

		location, err := time.LoadLocation("Asia/Jakarta")
		if err != nil {
			logToCSV([]string{"Timezone tidak tersedia"})
			return
		}

		// Log ke terminal (bukan ke CSV)
		log.Printf("Pesan diterima. Topik: %s, Payload: %s", m.Topic(), m.Payload())

		logData := []string{}
		dbInsertErrors := []string{}

		// Step 1: Parsing JSON
		startParsing := time.Now()
		var payload map[string]float64
		if err := json.Unmarshal(m.Payload(), &payload); err != nil {
			logData = append(logData, fmt.Sprintf("Parsing gagal: %v", err, time.Since(startParsing).Seconds()))
			logToCSV(logData)
			return
		}
		durationParsing := time.Since(startParsing)
		logData = append(logData, fmt.Sprintf("Parsing berhasil: %.10f", durationParsing.Seconds()))

		// Step 2: Insert per item
		for deviceId, value := range payload {
			startInsert := time.Now()
			currentTime := startInsert.In(location).Format("2006-01-02 15:04:05.000")
			query := "INSERT INTO `Value` (deviceId, value, created) VALUES (?, ?, ?)"
			_, err := db.Exec(query, deviceId, value, currentTime)
			durationInsert := time.Since(startInsert)

			if err != nil {
				dbInsertErrors = append(dbInsertErrors, deviceId)
				logData = append(logData, fmt.Sprintf("DB Insert Gagal: %s error: %v (%v detik)", deviceId, err, durationInsert.Seconds()))
			} else {
				logData = append(logData, fmt.Sprintf("%s %.10f %v", deviceId, value, durationInsert.Seconds()))
			}
		}

		// Step 3: Evaluasi hasil insert
		if len(dbInsertErrors) > 0 {
			logData = append(logData, fmt.Sprintf("Sensor gagal input: %v", dbInsertErrors))
		} else {
			logData = append(logData, "Semua data berhasil disimpan ke DB")
		}

		// Step 4: Total waktu eksekusi
		totalDuration := time.Since(startTime)
		logData = append(logData, fmt.Sprintf("Proses selesai: %.10f", totalDuration.Seconds()))

		// Step 5: Simpan ke CSV
		logToCSV(logData)
	}(msg)
}

var maxPayloadSize int

func logToCSV(data []string) {
	location, _ := time.LoadLocation("Asia/Jakarta")
	timestamp := time.Now().In(location).Format("02/01/06 15:04:05.000")

	filePath := "/home/sstk/HEB2024/dashboard-bms/be-1/log-insert/log.csv"
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Gagal membuat direktori: %v", err)
		return
	}

	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Gagal membuka file CSV: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if len(data) > maxPayloadSize {
		maxPayloadSize = len(data)
	}

	if !fileExists {
		header := []string{"Timestamp", "Parsing Time"}
		for i := 1; i <= maxPayloadSize-3; i++ {
			header = append(header, fmt.Sprintf("Insert %d", i))
		}
		header = append(header, "Insert Status", "Total Execution Time")
		writer.Write(header)
	}

	for len(data) < maxPayloadSize {
		data = append(data, "")
	}

	record := append([]string{timestamp}, data...)
	if err := writer.Write(record); err != nil {
		log.Printf("Gagal menulis log ke file CSV: %v", err)
	} else {
		log.Printf("Berhasil menulis log ke file CSV: %v", record)
	}
}

/* KODE PROGRAM - SEMUA PARAMETER */
func parameterHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	vars := mux.Vars(r)
	roomId := vars["roomId"]

	if roomId == "" {
		http.Error(w, `{"error": "roomId required"}`, http.StatusBadRequest)
		logIEQIndoor("-", []string{"roomId kosong/tidak diberikan"})
		return
	}

	validateStart := time.Now()

	// Step 1: Validasi selesai
	validateDuration := time.Since(validateStart).Seconds()
	fmt.Printf("Validasi selesai, durasi: %.10f detik\n", validateDuration)

	query := `
        SELECT v.value, s.alias AS parameter
        FROM Value v
        JOIN Parameter s ON v.deviceId = s.id
        JOIN Site si ON s.siteId = si.id
        WHERE si.alias = ?
        AND v.created = (SELECT MAX(created) FROM Value WHERE deviceId = s.id)
        ORDER BY s.alias
    `

	startQuery := time.Now()
	fmt.Println("Query started")
	rows, err := db.Query(query, roomId)
	if err != nil {
		http.Error(w, "Error fetching data from database", http.StatusInternalServerError)
		log.Println("Error fetching data:", err)
		logIEQIndoor(roomId, []string{"Error saat query DB"})
		return
	}
	defer rows.Close()

	// Step 2: Query Parameter berhasil
	queryDuration := time.Since(startQuery).Seconds()
	fmt.Printf("Durasi Query DB: %.10f detik\n", queryDuration)

	result := make(map[string]float64)

	// Step 3: Map terbentuk
	mapStart := time.Now()
	for rows.Next() {
		var value float64
		var parameter string
		if err := rows.Scan(&value, &parameter); err != nil {
			http.Error(w, "Error scanning database result", http.StatusInternalServerError)
			log.Println("Error scanning result:", err)
			logIEQIndoor(roomId, []string{"Error saat membaca hasil rows DB"})
			return
		}
		result[parameter] = value
	}
	mapDuration := time.Since(mapStart).Seconds()
	fmt.Printf("Map parameter berhasil terbentuk, durasi: %.10f detik\n", mapDuration)

	// Step 4: Pengiriman JSON
	jsonStart := time.Now()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, "Failed to encode JSON", http.StatusInternalServerError)
		log.Println("Failed to encode JSON:", err)
		logIEQIndoor(roomId, []string{"Gagal encode hasil ke JSON"})
		return
	}
	jsonDuration := time.Since(jsonStart).Seconds()
	fmt.Printf("JSON berhasil dikirim, durasi: %.10f detik\n", jsonDuration)

	// Step 5: Waktu total eksekusi
	totalDuration := time.Since(startTime).Seconds()
	fmt.Printf("Total Eksekusi: %.10f detik\n", totalDuration)

	// Simpan ke csv
	logIEQIndoor(roomId, []string{
		fmt.Sprintf("Validasi: %.10f", validateDuration), fmt.Sprintf("Query DB: %.10f", queryDuration),
		fmt.Sprintf("Map: %.10f", mapDuration), fmt.Sprintf("JSON: %.10f", jsonDuration), fmt.Sprintf("Total Eksekusi: %.10f", totalDuration),
	})
}

var logHeaders = []string{
	"Timestamp",
	"Site (roomId)",
	"Step 1 - Validasi SiteID",
	"Step 2 - Query Parameter",
	"Step 3 - Map Terbentuk",
	"Step 4 - JSON Dikirim",
	"Step 5 - Total Waktu Eksekusi",
}

func logIEQIndoor(roomId string, data []string) {
	location, _ := time.LoadLocation("Asia/Jakarta")
	timestamp := time.Now().In(location).Format("2006-01-02 15:04:05.000")

	filePath := "/home/sstk/HEB2024/dashboard-bms/be-1/log-responseIEQ/log-IEQ.csv"
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Gagal membuat direktori: %v", err)
		return
	}

	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Gagal membuka file CSV: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if !fileExists {
		writer.Write(logHeaders)
	}

	for len(data) < len(logHeaders)-2 {
		data = append(data, "")
	}

	record := append([]string{timestamp, roomId}, data...)
	if err := writer.Write(record); err != nil {
		log.Printf("Gagal menulis log ke file CSV: %v", err)
	} else {
		log.Printf("Berhasil menulis log ke file CSV: %v", record)
	}
}

/* KODE PROGRAM - GRAFIK HISTORIS */
func getDeviceIdByAlias(siteAlias, aliasDeviceID string, prevTime *time.Time) (string, []string, error) {
	query :=
		`	SELECT p.id
		FROM Parameter p
		JOIN Site s ON p.siteId = s.id
		WHERE s.alias = ? AND p.alias = ?`

	var deviceId string
	startQuery := time.Now()
	err := db.QueryRow(query, siteAlias, aliasDeviceID).Scan(&deviceId)

	logData := []string{}

	// Step 2 - Query Device ID
	if err != nil {
		if err == sql.ErrNoRows {
			logData = append(logData, fmt.Sprintf("Error: %v", err))
			return "", logData, fmt.Errorf("Lokasi atau parameter tidak ditemukan %s", aliasDeviceID)
		}
		log.Printf("Error retrieving deviceId from database: %v", err)
		logData = append(logData, fmt.Sprintf("Query Device ID Gagal: %v", err))
	} else {
		queryDuration := time.Since(startQuery)
		logData = append(logData, fmt.Sprintf("Step 2 - Query Device ID: %.10f", queryDuration.Seconds()))
	}

	return deviceId, logData, nil
}

func getLatestData(deviceId string, prevTime *time.Time) ([]map[string]interface{}, []string, error) {
	query := `SELECT v.value, v.created FROM Value v WHERE v.deviceId = ? ORDER BY v.created DESC LIMIT 30;`
	startQuery := time.Now()
	rows, err := db.Query(query, deviceId)
	if err != nil {
		log.Printf("Error querying data: %v", err)
		return nil, nil, err
	}
	defer rows.Close()

	logData := []string{}

	// Step 3 - Query Data Terakhir
	queryDuration := time.Since(startQuery)
	logData = append(logData, fmt.Sprintf("Step 3 - Query Data Terakhir: %.10f", queryDuration.Seconds()))

	var data []map[string]interface{}
	for rows.Next() {
		var value float64
		var created time.Time
		if err := rows.Scan(&value, &created); err != nil {
			log.Printf("Error scanning row: %v", err)
			return nil, logData, err
		}

		data = append(data, map[string]interface{}{
			"value": value,
			"date":  created.Format("2006/01/02"),
			"time":  created.Format("15:04:05"),
		})
	}

	// Step 5 - Parse Row Into Data
	addLogStep(&logData, "Step 4 - Pembentukan map:", prevTime)

	return data, logData, nil
}

func getHistory(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now().In(time.FixedZone("Asia/Jakarta", 7*3600))
	prevTime := startTime

	vars := mux.Vars(r)
	siteAlias := vars["siteAlias"]
	aliasDeviceID := vars["aliasDeviceID"]

	if siteAlias == "" || aliasDeviceID == "" {
		http.Error(w, "Alias Site dan Device belum anda masukkan.", http.StatusBadRequest)
		logHistory(siteAlias, aliasDeviceID, []string{"Alias kosong di URL"})
		return
	}

	logData := []string{}

	// Step 1 - Validasi ID
	addLogStep(&logData, "Step 1 - Validasi Alias:", &prevTime)

	deviceId, stepLog, err := getDeviceIdByAlias(siteAlias, aliasDeviceID, &prevTime)
	logData = append(logData, stepLog...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		logHistory(siteAlias, aliasDeviceID, logData)
		return
	}

	data, stepLog, err := getLatestData(deviceId, &prevTime)
	logData = append(logData, stepLog...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		logHistory(siteAlias, aliasDeviceID, logData)
		return
	}

	// Step 4 - Response JSON dikirim = Step getLatestData
	addLogStep(&logData, "Step 5 - Response JSON dikirim:", &prevTime)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	logData = append(logData, fmt.Sprintf("Total: %.10f", prevTime.Sub(startTime).Seconds()))

	logHistory(siteAlias, aliasDeviceID, logData)
}

var logHeadersHistory = []string{
	"Timestamp",
	"Site (roomId)",
	"Alias Device",
	"Step 1 - Validasi Alias",
	"Step 2 - Query Device ID",
	"Step 3 - Query Latest Data",
	"Step 4 - Map Terbentuk",
	"Step 5 - Response JSON",
}

func logHistory(siteAlias string, aliasDeviceID string, data []string) {
	location, _ := time.LoadLocation("Asia/Jakarta")
	timestamp := time.Now().In(location).Format("2006-01-02 15:04:05.000")

	filePath := "/home/sstk/HEB2024/dashboard-bms/be-1/log-resp-history/log-resp-history.csv"
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Gagal membuat direktori: %v", err)
		return
	}

	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Gagal membuka file CSV: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if !fileExists {
		writer.Write(logHeadersHistory)
	}

	for len(data) < len(logHeadersHistory)-1 {
		data = append(data, "")
	}

	record := append([]string{timestamp, siteAlias, aliasDeviceID}, data...)
	if err := writer.Write(record); err != nil {
		log.Printf("Gagal menulis log ke file CSV: %v", err)
	} else {
		log.Printf("Berhasil menulis log ke file CSV: %v", record)
	}
}

func addLogStep(logData *[]string, step string, prevTime *time.Time) {
	stepTime := time.Now()
	duration := stepTime.Sub(*prevTime).Seconds()
	*logData = append(*logData, fmt.Sprintf("%s: %.10f", step, duration))
	*prevTime = stepTime
}

func main() {
	// Inisialisasi database
	localDB := initDB()

	db = localDB

	defer localDB.Close()

	initMQTT()

	// Inisialisasi router
	apiRouter := mux.NewRouter()

	// Tambahkan rute lainnya
	apiRouter.HandleFunc("/api/monitoring/{roomId}", parameterHandler).Methods("GET")
	apiRouter.HandleFunc("/api/grafik/{siteAlias}/{aliasDeviceID}", getHistory).Methods("GET")

	// Middleware CORS
	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://xxx:10006", "http://xxx:10006", "http://xxx:10006"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization", "X-Requested-With"},
		AllowCredentials: true,
	}).Handler(apiRouter)

	// Main multiplexer
	mainMux := http.NewServeMux()
	mainMux.Handle("/", corsMiddleware)

	log.Println("Server is running on port 10004...")
	log.Fatal(http.ListenAndServe(":10004", mainMux))
}
