package main

import (
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/opticSquid/ticksim/gen/marketdata/v1/marketdataconnect"
	"github.com/opticSquid/ticksim/internal/marketdataserver"
)

func main() {
	// 1. Define the base data path pointing to the data directory from root
	baseDataPath := "../../data"

	// Verify the data directory exists
	if _, err := os.Stat(baseDataPath); os.IsNotExist(err) {
		slog.Error("Data directory does not exist. Ensure you run the server from the project root.", "data-dir", baseDataPath)
		os.Exit(1)
	}

	// 1. Initialize your MarketDataServer
	// Ensure the baseDataPath field name matches the case used in your model.go struct (e.g., baseDataPath or BaseDataPath)
	server := &marketdataserver.MarketDataServer{
		BaseDataPath: baseDataPath,
	}

	// 2. Set up a standard Go HTTP multiplexer
	mux := http.NewServeMux()

	// 3. Register your Connect service on the multiplexer
	// This maps the generated routing path (e.g., /aerotrade.marketdata.v1.MarketDataService/) to your implementation
	path, handler := marketdataconnect.NewMarketDataServiceHandler(server)
	mux.Handle(path, handler)

	port := ":50051"
	fmt.Printf("Connect Market Data Server running on http://localhost%s\n", port)

	srv := &http.Server{
		Addr:    port,
		Handler: mux,
	}
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(true)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start server: %v", err)
	}
}
