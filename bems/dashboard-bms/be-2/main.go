package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // Import driver MariaDB/MySQL
	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/cors"
)

const (
	minioEndpoint        = "xxx"
	minioAccessKey       = "root"
	minioSecretKey       = "xxx"
	minioBucketName      = "heb2024"
	minioFileUploadPath  = "heb2024/model"
	databaseDSN          = "root:xxx@tcp(xxx)/xxx"
	allowedFileExtension = ".joblib"
	validParams          = "temperature,humidity,wind_speed,light_intensity"
	validLocations       = "indoor,outdoor"
)

var (
	minioClient          *minio.Client
	db                   *sql.DB
	indonesiaLocation, _ = time.LoadLocation("Asia/Jakarta")
)

type Selection struct {
	SiteID    string `json:"siteId"`
	Parameter string `json:"parameter"`
	Model     string `json:"model"`
}

type SiteInfo struct {
	SiteAlias string `json:"siteId"`
	Parameter string `json:"parameter"`
	Model     string `json:"model"`
}

type APIResponse struct {
	Message string `json:"message"`
}

func init() {
	// Initialize MinIO Client
	var err error
	minioClient, err = minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("Failed to initialize MinIO: %v", err)
	}
	log.Println("Berhasil koneksi ke penyimpanan objek")

	// Initialize Database Connection
	db, err = sql.Open("mysql", databaseDSN)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("Database is unreachable: %v", err)
	}
	log.Println("Berhasil koneksi ke basis data")
}

func submitSelection(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	siteId := r.FormValue("siteId")
	parameter := r.FormValue("parameter")
	model := r.FormValue("model")

	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM models_metadata
		WHERE parameter = ? AND model_name = ? AND siteId = ?
	`, parameter, model, siteId).Scan(&count)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error checking metadata: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	if count == 0 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Belum ada model yang disimpan untuk parameter maupun lokasi ini.",
		})
		return
	}

	_, err = db.Exec(`
		INSERT INTO models (selected_at, siteId, parameter, model) 
		VALUES (?, ?, ?, ?)
	`, time.Now(), siteId, parameter, model)

	if err != nil {
		http.Error(w, fmt.Sprintf("Error saving data: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(APIResponse{Message: "Sukses"})
}

func postHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Total ukuran metadata dan model lebih dari 10 MB"})
		return
	}

	file, fileHeader, err := r.FormFile("model")
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Tidak ada objek model yang diunggah"})
		return
	}
	defer file.Close()

	log.Printf("File diterima: %s", fileHeader.Filename)

	siteId := r.FormValue("siteId")
	parameter := r.FormValue("parameter")
	metadata := r.FormValue("metadata")
	meta_site := r.FormValue("meta_site")

	if !strings.HasSuffix(fileHeader.Filename, allowedFileExtension) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Ekstensi file tidak valid"})
		return
	}

	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM Site WHERE id = ?);"
	if err := db.QueryRow(query, siteId).Scan(&exists); err != nil || !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Lokasi tidak tersedia"})
		return
	}

	if !strings.Contains(validParams, parameter) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Parameter tidak valid"})
		return
	}

	objectName := minioFileUploadPath + "/" + fileHeader.Filename
	log.Printf("Mengunggah file ke MinIO: %s", objectName)
	_, err = minioClient.PutObject(r.Context(), minioBucketName, objectName, file, fileHeader.Size, minio.PutObjectOptions{})
	if err != nil {
		log.Printf("Gagal mengunggah file ke MinIO: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Gagal mengunggah file ke MinIO"})
		return
	}

	insertQuery := `
    INSERT INTO models_metadata (metadata, meta_site, model_name, parameter, siteId)
    VALUES (?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        metadata = VALUES(metadata),
        meta_site = VALUES(meta_site),
        parameter = VALUES(parameter),
        siteId = VALUES(siteId)
	`

	log.Printf("Menjalankan SQL: %s", insertQuery)

	if _, err := db.Exec(insertQuery, metadata, meta_site, fileHeader.Filename, parameter, siteId); err != nil {
		log.Printf("Gagal menyimpan data ke database: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Gagal menyimpan data ke database"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(APIResponse{Message: "Sukses"})
}

func getImage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileName := vars["fileName"]

	client, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		http.Error(w, "Failed to connect to MinIO", http.StatusInternalServerError)
		log.Println("Error: Failed to connect to MinIO")
		return
	}

	object, err := client.GetObject(r.Context(), minioBucketName, "gambar/"+fileName, minio.GetObjectOptions{})
	if err != nil {
		http.Error(w, "Failed to retrieve file", http.StatusInternalServerError)
		log.Printf("Error: Failed to retrieve file '%s': %v", fileName, err)
		return
	}
	defer object.Close()

	log.Printf("Successfully retrieved file: %s", fileName)
	w.Header().Set("Content-Type", "image/jpeg")
	_, err = io.Copy(w, object)
	if err != nil {
		http.Error(w, "Failed to send file", http.StatusInternalServerError)
		log.Println("Error: Failed to send file")
	}
}

func getActiveModels(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	rows, err := db.Query(`
        SELECT DISTINCT si.alias AS site_alias, m.parameter, m.model_name
        FROM models_metadata m
        JOIN Parameter p ON p.alias = m.parameter
        JOIN Site si ON si.id = m.siteId
        WHERE si.alias IS NOT NULL;
    `)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying database: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var siteInfos []SiteInfo
	for rows.Next() {
		var siteAlias, parameter, modelName string
		if err := rows.Scan(&siteAlias, &parameter, &modelName); err != nil {
			http.Error(w, fmt.Sprintf("Error reading result: %s", err.Error()), http.StatusInternalServerError)
			return
		}

		siteInfos = append(siteInfos, SiteInfo{
			SiteAlias: siteAlias,
			Parameter: parameter,
			Model:     modelName,
		})
	}

	if err := rows.Err(); err != nil {
		http.Error(w, fmt.Sprintf("Error during rows iteration: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	if len(siteInfos) == 0 {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Tidak ada model aktif",
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(siteInfos)
}

func addLogStep(logData *[]string, step string, prevTime *time.Time) {
	location, _ := time.LoadLocation("Asia/Jakarta")
	stepTime := time.Now().In(location)
	*logData = append(*logData, fmt.Sprintf("Jam: %s. Selisih: %v", stepTime.Format("15:04:05.000000"), stepTime.Sub(*prevTime)))
	*prevTime = stepTime
}

// Sementara untuk getHeatmap, hapus s nya kalau butuh
func getHeatmap(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now().In(time.FixedZone("Asia/Jakarta", 7*3600))
	prevTime := startTime
	logData := []string{}

	vars := mux.Vars(r)
	parameter := vars["parameter"]
	roomId := vars["roomId"]

	addLogStep(&logData, "Step 1 - Ambil Parameter URL", &prevTime)

	prefix := fmt.Sprintf("heatmap/%s_%s", roomId, parameter)

	client, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		http.Error(w, "Failed to connect to MinIO", http.StatusInternalServerError)
		log.Println("Error: Failed to connect to MinIO")
		logData = append(logData, "Error: Gagal koneksi ke MinIO")
		logHeatmap(roomId, parameter, logData)
		return
	}

	addLogStep(&logData, "Step 2 - Koneksi MinIO Berhasil", &prevTime)

	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	}

	// Step 3 - List Objects
	listStart := time.Now() // Catat waktu sebelum listing objek
	var lastObject minio.ObjectInfo
	found := false
	for objectInfo := range client.ListObjects(r.Context(), minioBucketName, opts) {
		if objectInfo.Err != nil {
			log.Printf("Error saat listing objek: %v", objectInfo.Err)
			logData = append(logData, fmt.Sprintf("Error saat listing objek: %v", objectInfo.Err))
			continue
		}
		lastObject = objectInfo
		found = true
	}
	// Durasi proses ListObjects
	listDuration := time.Since(listStart).Seconds() // Durasi dalam detik
	logData = append(logData, fmt.Sprintf("Step 3 - List Object Selesai: %.10f", listDuration))

	if !found {
		http.Error(w, "Tidak ada gambar ditemukan", http.StatusNotFound)
		logData = append(logData, "Gambar tidak ditemukan")
		logHeatmap(roomId, parameter, logData)
		return
	}

	// Step 4 - Ambil dan Kirim File
	step4Start := time.Now() // Catat waktu sebelum pengambilan file
	object, err := client.GetObject(r.Context(), minioBucketName, lastObject.Key, minio.GetObjectOptions{})
	if err != nil {
		http.Error(w, "Gagal mengambil file", http.StatusInternalServerError)
		logData = append(logData, fmt.Sprintf("Error saat mengambil file: %v", err))
		logHeatmap(roomId, parameter, logData)
		return
	}
	defer object.Close()

	// Durasi pengambilan file
	step4Duration := time.Since(step4Start).Seconds() // Durasi pengambilan file dalam detik
	logData = append(logData, fmt.Sprintf("Step 4 - Berhasil ambil objek: %.10f", step4Duration))

	// Mengirim file ke response
	log.Printf("Berhasil mengambil file terakhir: %s", lastObject.Key)
	w.Header().Set("Content-Type", "image/png")

	// Waktu mulai mengirim file
	copyStart := time.Now() // Waktu mulai pengiriman file
	if _, err := io.Copy(w, object); err != nil {
		http.Error(w, "Gagal mengirim file", http.StatusInternalServerError)
		logData = append(logData, fmt.Sprintf("Error saat mengirim file: %v", err))
		logHeatmap(roomId, parameter, logData)
		return
	}

	// Durasi pengiriman file
	copyDuration := time.Since(copyStart).Seconds() // Durasi pengiriman file dalam detik
	logData = append(logData, fmt.Sprintf("Step 5 - Objek dikirim: %.10f", copyDuration))

	// Total waktu eksekusi
	logData = append(logData, fmt.Sprintf("Total: %.10f", time.Since(startTime).Seconds()))
	logHeatmap(roomId, parameter, logData)
}

var logHeadersHeatmap = []string{
	"Timestamp",
	"Room ID",
	"Parameter",
	"Step 1 - Ambil URL",
	"Step 2 - Koneksi MinIO",
	"Step 3 - List Objek",
	"Step 4 - Ambil objek",
	"Step 5 - Kirim objek",
	"Total Eksekusi",
}

func logHeatmap(roomId string, parameter string, data []string) {
	location, _ := time.LoadLocation("Asia/Jakarta")
	timestamp := time.Now().In(location).Format("2006-01-02 15:04:05.000")

	filePath := "/home/sstk/HEB2024/dashboard-bms/be-2/log-heatmap/log-heatmap.csv"
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Gagal membuat direktori log heatmap: %v", err)
		return
	}

	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Gagal membuka file log heatmap: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if !fileExists {
		writer.Write(logHeadersHeatmap)
	}

	for len(data) < len(logHeadersHeatmap)-3 {
		data = append(data, "")
	}

	record := append([]string{timestamp, roomId, parameter}, data...)
	if err := writer.Write(record); err != nil {
		log.Printf("Gagal menulis log heatmap ke CSV: %v", err)
	} else {
		log.Printf("Berhasil log heatmap: %v", record)
	}
}

func uploadImage(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(10 << 20) // Limit to 10 MB
	if err != nil {
		http.Error(w, "File too large or invalid form data", http.StatusBadRequest)
		log.Printf("Error: ParseMultipartForm failed - %v", err)
		return
	}

	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to retrieve file. Ensure 'file' field is included in the form-data.", http.StatusBadRequest)
		log.Printf("Error: Failed to retrieve file - %v", err)
		return
	}
	defer file.Close()

	log.Printf("File received: %s (%d bytes)", fileHeader.Filename, fileHeader.Size)

	deviceID := r.FormValue("deviceId")
	if deviceID == "" {
		http.Error(w, "Missing deviceId", http.StatusBadRequest)
		log.Println("Error: Missing deviceId")
		return
	}

	siteAlias, err := getSiteAlias(deviceID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get site alias: %v", err), http.StatusBadRequest)
		log.Printf("Error: Failed to get site alias for device ID '%s': %v", deviceID, err)
		return
	}

	fileData := new(bytes.Buffer)
	_, err = io.Copy(fileData, file)
	if err != nil {
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		log.Println("Error: Failed to read file")
		return
	}

	timestamp := time.Now().In(indonesiaLocation).Format("2006-01-02_15-04")
	extension := "jpg"
	fileNameWithTimestamp := fmt.Sprintf("gambar/%s/%s.%s", siteAlias, timestamp, extension)
	fileNameWithAlias := fmt.Sprintf("gambar/%s.%s", siteAlias, extension)

	_, err = minioClient.PutObject(r.Context(), minioBucketName, fileNameWithTimestamp, bytes.NewReader(fileData.Bytes()), int64(fileData.Len()), minio.PutObjectOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to upload file (timestamp): %v", err), http.StatusInternalServerError)
		log.Printf("Error: Failed to upload file with timestamp '%s': %v", fileNameWithTimestamp, err)
		return
	}
	log.Printf("Successfully uploaded file with timestamp: %s", fileNameWithTimestamp)

	_, err = minioClient.PutObject(r.Context(), minioBucketName, fileNameWithAlias, bytes.NewReader(fileData.Bytes()), int64(fileData.Len()), minio.PutObjectOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to upload file (alias): %v", err), http.StatusInternalServerError)
		log.Printf("Error: Failed to upload file with alias '%s': %v", fileNameWithAlias, err)
		return
	}
	log.Printf("Successfully uploaded file with alias: %s", fileNameWithAlias)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(APIResponse{Message: "Success"})
}

func getSiteAlias(deviceID string) (string, error) {
	var alias string
	query := `
		SELECT s.alias FROM Site s JOIN Parameter p ON p.siteId = s.id WHERE p.id = ?;
	`
	err := db.QueryRow(query, deviceID).Scan(&alias)
	if err != nil {
		return "", err
	}
	return alias, nil
}

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/upload", uploadImage).Methods("POST")
	r.HandleFunc("/api/selection", submitSelection).Methods("POST")
	r.HandleFunc("/api/post", postHandler).Methods("POST")
	r.HandleFunc("/monitoring/{fileName}", getImage).Methods("GET")
	r.HandleFunc("/api/available-models", getActiveModels).Methods("GET")
	r.HandleFunc("/api/heatmap/{roomId}/{parameter}", getHeatmap).Methods("GET")

	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://10.46.7.51:10006", "http://localhost:10006", "http://172.35.0.7:10006"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization", "X-Requested-With"},
		AllowCredentials: true,
	}).Handler(r)

	http.Handle("/", corsMiddleware)
	log.Fatal(http.ListenAndServe(":10005", nil))
}
