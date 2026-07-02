package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	pb "github.com/opticSquid/ticksim/gen/marketdata/v1"
)

func (s *MarketDataServer) StreamMarketData(ctx context.Context, req *connect.Request[pb.StreamMarketDataRequest], stream *connect.ServerStream[pb.StreamMarketDataResponse]) error {
	symbols := req.Msg.Symbols
	startTime := req.Msg.StartTime
	endTime := req.Msg.EndTime

	// Initialize random seed for deterministic or variable noise matching your requirements
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, symbol := range symbols {
		symbol = strings.TrimSpace(symbol)
		filepath := fmt.Sprintf("%s/01-07-2025-TO-01-07-2026-%s-ALL-N.csv", s.baseDataPath, symbol)
		dailyRecords, err := s.parseCSVAndFilter(filepath, symbol, startTime, endTime)
	}

	resp := &pb.StreamMarketDataResponse{ /* your payload */ }
	if err := stream.Send(resp); err != nil {
		return err
	}

	return nil

}

func (s *MarketDataServer) parseCSVAndFilter(filepath, symbol string, startTime, endTime time.Time) ([]DailyBar, error) {

	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	_, err = reader.Read()
	if err != nil {
		return nil, err
	}

	var validDays []DailyBar

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		dateStr := strings.TrimSpace(row[2])
		parsedDate, err := time.Parse("02-Jan-2006", dateStr)
		if err != nil {
			continue // Skip corrupt or misformatted data entries safely
		}
		if parsedDate.Before(startTime) || parsedDate.After(endTime) {
			continue // Skip dates outside the requested range
		}

		// Helper to sanitize fields containing commas like "22,30,098" or "1,786.90"
		cleanFloat := func(field string) float64 {
			c := strings.ReplaceAll(strings.TrimSpace(field), ",", "")
			val, _ := strconv.ParseFloat(c, 64)
			return val
		}

		cleanUint := func(field string) uint64 {
			c := strings.ReplaceAll(strings.TrimSpace(field), ",", "")
			val, _ := strconv.ParseUint(c, 10, 64)
			return val
		}

		bar := DailyBar{
			Symbol:       symbol,
			Date:         parsedDate,
			Open:         cleanFloat(row[4]),
			High:         cleanFloat(row[5]),
			Low:          cleanFloat(row[6]),
			LastPrice:    cleanFloat(row[7]),
			Close:        cleanFloat(row[8]),
			AveragePrice: cleanFloat(row[9]),
			Volume:       cleanUint(row[10]),
			NoOfTrades:   cleanUint(row[12]),
		}
		validDays = append([]DailyBar{bar}, validDays...)
	}

	return validDays, nil

}
