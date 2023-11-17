package middleware

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"stocksAPI/models"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type response struct {
	ID      int64  `json:"id,omitempty"`
	Message string `json:"message,omitempty"`
}

func createConnection() *sql.DB {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("error loading .env file", err)
	}
	var (
		host     = os.Getenv("HOST")
		port     = os.Getenv("PORT")
		user     = os.Getenv("USER")
		password = os.Getenv("PASSWORD")
		dbname   = os.Getenv("DB_NAME")
	)

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Panic("error opening db", err)
	}

	err = db.Ping()
	if err != nil {
		log.Panic("error while pinging the DB", err)
	}

	fmt.Println("Successfully connected with Postgresql DB")

	return db
}

func GetStock(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		log.Fatalf("Unable to convert string to integer, %v", err)
	}
	stock, err := getStock(int64(id))
	if err != nil {
		log.Fatalf("unable to get stocks. %v", err)
	}

	json.NewEncoder(w).Encode(stock)
}

func GetAllStock(w http.ResponseWriter, r *http.Request) {
	stocks, err := getAllStock()
	if err != nil {
		log.Fatalf("unable to fetch all stocks. %v", err)
	}

	json.NewEncoder(w).Encode(stocks)
}

func CreateStock(w http.ResponseWriter, r *http.Request) {
	var stock models.Stock

	err := json.NewDecoder(r.Body).Decode(&stock)
	if err != nil {
		log.Fatalf("Unable to decode the request body. %v", err)
	}

	insertID := insertStock(stock)

	res := response{
		ID:      insertID,
		Message: "stock created successfully",
	}

	json.NewEncoder(w).Encode(res)
}

func UpdateStock(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	id, err := strconv.Atoi(params["id"])
	if err != nil {
		log.Fatalf("unable to convert string to int. %v", err)
	}

	var stock models.Stock
	err = json.NewDecoder(r.Body).Decode(&stock)
	if err != nil {
		log.Fatalf("unable to decode the request. %v", err)
	}

	updatedRows := updateStock(int64(id), stock)
	msg := fmt.Sprintf("Stock updated successfully. Total rows/records addected %v", updatedRows)
	res := response{
		ID:      int64(id),
		Message: msg,
	}

	json.NewEncoder(w).Encode(res)
}

func DeleteStock(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		log.Fatalf("error while parsing interger. %v", err)
	}

	deletedRows := deleteStock(int64(id))
	if err != nil {
		log.Fatalf("unable to delete the stock. %v", err)
	}

	msg := fmt.Sprintf("Stock deleted successfully. Total rows/records affected: %v", deletedRows)
	res := response{
		ID:      int64(id),
		Message: msg,
	}

	json.NewEncoder(w).Encode(res)

}

func insertStock(stock models.Stock) int64 {
	db := createConnection()
	defer db.Close()
	sqlStatement := `INSERT INTO stocks(name, price, company) VALUES ($1, $2, $3) RETURNING stockid`
	var id int64

	err := db.QueryRow(sqlStatement, stock.Name, stock.Price, stock.Company).Scan(&id)
	if err != nil {
		log.Fatalf("unable to execute the query. %v", err)
	}
	fmt.Printf("Inserted a single record %v\n", id)
	return id
}

func getStock(id int64) (models.Stock, error) {
	db := createConnection()
	defer db.Close()

	var stock models.Stock
	sqlStatement := `SELECT * FROM stocks WHERE stockid=$1`
	row := db.QueryRow(sqlStatement, id)
	err := row.Scan(&stock.StockID, &stock.Name, &stock.Price, &stock.Company)

	switch err {
	case sql.ErrNoRows:
		fmt.Printf("No rows were returned. %v\n", err)
		return stock, nil
	case nil:
		return stock, nil
	default:
		log.Fatalf("Unable to scan the row")
	}

	return stock, err
}

func getAllStock() ([]models.Stock, error) {
	db := createConnection()
	defer db.Close()

	var stocks []models.Stock
	sqlStatement := `SELECT * FROM stocks`

	rows, err := db.Query(sqlStatement)
	if err != nil {
		log.Fatalf("unable to execute the query. %v", err)
	}

	defer rows.Close()

	for rows.Next() {
		var stock models.Stock
		err := rows.Scan(&stock.StockID, &stock.Name, &stock.Price, &stock.Company)
		if err != nil {
			log.Fatalf("unable to scan the row. %v", err)
		}

		stocks = append(stocks, stock)
	}
	return stocks, err
}

func updateStock(id int64, stock models.Stock) int64 {
	db := createConnection()
	defer db.Close()

	sqlStatement := `UPDATE stocks SET name=$2, price=$3, company=$4 WHERE stockid=$1`
	res, err := db.Exec(sqlStatement, id, stock.Name, stock.Price, stock.Company)
	if err != nil {
		log.Fatalf("Unable to execute the query. %v", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatalf("error while checking the affected rows. %v", err)
	}

	fmt.Printf("Total rows/records affected %v", rowsAffected)
	return rowsAffected
}

func deleteStock(id int64) int64 {
	db := createConnection()
	defer db.Close()

	sqlStatement := `DELETE FROM stocks WHERE stockid=$1`
	res, err := db.Exec(sqlStatement, id)
	if err != nil {
		log.Fatalf("unable to execute the delete query. %v", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatalf("Error while checking the affected rows. %v", err)
	}
	fmt.Printf("Total rows/records affected %v\n", rowsAffected)

	return rowsAffected
}
