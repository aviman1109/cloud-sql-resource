package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type JSONSource struct {
	Source struct {
		UserName   string `json:"user"`
		Password   string `json:"pass"`
		HostName   string `json:"host"`
		Database   string `json:"database"`
		PrivateKey string `json:"private_key"`
	} `json:"source"`
	Parameters struct {
		Query string `json:"query"`
	} `json:"params"`
}
type Version struct {
	Version string `json:"version"`
}
type Metadata struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
type Output struct {
	Version  Version    `json:"version"`
	Metadata []Metadata `json:"metadata"`
}

func ExecuteCloudSQLProxy(input JSONSource) (*os.Process, error) {
	log.SetFlags(0)
	// Execute the Cloud SQL Proxy command
	// Ref: https://cloud.google.com/sql/docs/mysql/connect-auth-proxy
	cmd := exec.Command("/opt/resource/cloud-sql-proxy", input.Source.HostName, "--unix-socket", "/cloudsql", "--json-credentials", string(input.Source.PrivateKey))
	// Create a pipe to capture stdout and stderr
	stdoutPipe, _ := cmd.StdoutPipe()

	// Start the command
	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	// Set up a timer to break the loop after 5 seconds
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	// Wait for the Cloud SQL Proxy to start and check for success message
	reader := bufio.NewReader(stdoutPipe)
loop:
	for {
		select {
		case <-timer.C:
			log.Println("Command output:")
			io.Copy(os.Stdout, reader)
			log.Println("Timed out waiting for the Cloud SQL Proxy to start")
			// Send SIGINT signal to shut down the process
			cmd.Process.Signal(os.Interrupt)
			break loop
		default:
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return nil, err
			}
			if strings.Contains(line, "is ready for new connections!") {
				// cloud-sql-proxy should print "The proxy has started successfully and is ready for new connections!"
				break loop
			}
			if strings.Contains(line, "error") {
				log.Panicf(line)
				// Send SIGINT signal to shut down the process
				cmd.Process.Signal(os.Interrupt)
				break loop
			}
		}
	}

	return cmd.Process, nil
}
func CheckDatabaseConnection(input JSONSource) (*sql.DB, error) {
	log.SetFlags(0)
	// Set up the connection string.
	connString := fmt.Sprintf("%s:%s@unix(/cloudsql/%s)/%s", input.Source.UserName, fmt.Sprintf(input.Source.Password), input.Source.HostName, input.Source.Database)
	// Open the connection.
	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, err
	}

	// Check the connection.
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
func QuerySQL(db *sql.DB, queryCommands string) error {

	log.Println(queryCommands)
	// Start the timer.
	start := time.Now()

	// Query the database.
	rows, err := db.Query(queryCommands)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get the column names.
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Print the SQL query execution time.
	elapsed := time.Since(start)
	log.Printf("SQL query took %s\n", elapsed)

	// Print the column names.
	var header []string
	for _, col := range columns {
		header = append(header, col)
	}
	log.Println(strings.Join(header, " | "))

	// Loop through the rows and print the columns.
	for rows.Next() {
		// Create a slice of pointers to interface{} values to hold the column values.
		values := make([]interface{}, len(columns))
		for i := range columns {
			values[i] = new(interface{})
		}

		// Scan the row values into the values slice.
		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		// Print the row values.
		var row []string
		for _, col := range values {
			switch v := *(col.(*interface{})); v.(type) {
			case []byte:
				row = append(row, string(v.([]byte)))
			case nil:
				row = append(row, "<nil>")
			default:
				row = append(row, fmt.Sprintf("%v", v))
			}
		}
		log.Println(strings.Join(row, " | "))
	}

	return nil
}

func main() {
	log.SetFlags(0)

	var input JSONSource
	// Decode JSON from stdin into input struct
	decoder := json.NewDecoder(os.Stdin)
	err := decoder.Decode(&input)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	proxyProcess, err := ExecuteCloudSQLProxy(input)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	db, err := CheckDatabaseConnection(input)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	// Split the SQL commands into individual statements.
	statements := strings.Split(input.Parameters.Query, ";")

	// Loop through the statements and execute each UPDATE query.
	for _, stmt := range statements {
		// Skip any empty statements.
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		err = QuerySQL(db, stmt)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
	}

	output := Output{
		Version:  Version{Version: "static"},
		Metadata: []Metadata{},
	}

	// print output as JSON
	encodedOutput, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	fmt.Println(string(encodedOutput))

	// Send SIGINT signal to shut down the process
	proxyProcess.Signal(os.Interrupt)
}
