package marketdataserver

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	pb "github.com/opticSquid/ticksim/gen/marketdata/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *MarketDataServer) StreamMarketData(ctx context.Context, req *pb.StreamMarketDataRequest, stream *connect.ServerStream[pb.StreamMarketDataResponse]) error {
	symbols := req.Symbols
	slog.Info("Starting real-time dynamic market data generation", "symbols", symbols)
	startDate := req.StartDate
	endDate := req.EndDate

	// Initialize random seed for deterministic or variable noise matching your requirements
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, symbol := range symbols {
		symbol = strings.TrimSpace(symbol)
		filepath := fmt.Sprintf("%s/01-07-2025-TO-01-07-2026-%s-ALL-N.csv", s.BaseDataPath, symbol)

		dailyRecords, err := s.parseCSVAndFilter(filepath, symbol, startDate.AsTime(), endDate.AsTime())
		if err != nil {
			slog.Error("data parsing error", "symbol", symbol, "error", err)
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("data parsing failed for symbol %s", symbol))
		}

		// If a symbol registered later and has no entries inside the requested range,
		// dailyRecords will be empty. It gracefully omits streaming for this symbol.
		if len(dailyRecords) == 0 {
			continue
		}

		var sequenceNumber uint64 = 1
		for _, day := range dailyRecords {
			// Establish market open and close timestamps in UTC for this specific day
			// 9:15 AM IST = 03:45 UTC | 3:30 PM IST = 10:00 UTC
			marketOpen := time.Date(day.Date.Year(), day.Date.Month(), day.Date.Day(), 3, 45, 0, 0, time.UTC)
			marketClose := time.Date(day.Date.Year(), day.Date.Month(), day.Date.Day(), 10, 0, 0, 0, time.UTC)
			// Generate ticks. Let's simulate a tick arriving every 5 seconds of market time.
			// Total market duration = 6 hours 15 mins = 22,500 seconds.
			// 22500 / 5 = 4500 ticks per day.
			tickInterval := 5 * time.Second
			currentTickTime := marketOpen

			// Set the initial rolling price to the Day's Open Price
			rollingPrice := day.Open

			// Split the total daily trades across our generated ticks safely
			tradesPerStick := uint32(day.NoOfTrades / 4500)
			if tradesPerStick == 0 {
				tradesPerStick = 1
			}

			for currentTickTime.Before(marketClose) || currentTickTime.Equal(marketClose) {
				// 1. High-Performance Context Safety check
				select {
				case <-ctx.Done():
					return ctx.Err() // Kill execution immediately if client drops out
				default:
				}
				var tickPrice float64
				if currentTickTime.Equal(marketOpen) {
					tickPrice = day.Open
				} else if currentTickTime.Equal(marketClose) {
					tickPrice = day.Close
				} else {
					// Generate a random walk deviation bounded between -0.5% and +0.5%
					percentageChange := (r.Float64() * 0.01) - 0.005
					rollingPrice = rollingPrice * (1.0 + percentageChange)

					// Strict Clamping Rule: Ensure the random value never breaches historical bounds
					if rollingPrice > day.High {
						rollingPrice = day.High
					}
					if rollingPrice < day.Low {
						rollingPrice = day.Low
					}
					tickPrice = rollingPrice
				}

				resp := &pb.StreamMarketDataResponse{
					Symbol:         day.Symbol,
					SequenceNumber: sequenceNumber,
					Timestamp:      timestamppb.New(currentTickTime),
					Payload: &pb.StreamMarketDataResponse_Tick{
						Tick: &pb.TickData{
							BidPrice:       tickPrice - 0.05,
							AskPrice:       tickPrice + 0.05,
							LastTradePrice: tickPrice,
							LastTradeSize:  tradesPerStick,
						},
					},
				}
				slog.Debug("sent tick", "symbol", day.Symbol, "sequenceNumber", sequenceNumber, "timestamp", currentTickTime)
				// 4. Send packet down the wire
				if err := stream.Send(resp); err != nil {
					return connect.NewError(connect.CodeUnavailable, fmt.Errorf("failed to send tick stream: %w", err))
				}
				sequenceNumber++

				// 5. Playback speed management
				// A multiplier of 1.0 means we scale down the 5-second interval to a brief pause (e.g., 1 millisecond)
				// to allow the simulation engine to process a full day in a few seconds.
				sleepDuration := time.Duration(float64(tickInterval.Milliseconds())/float64(req.PlaybackSpeedMultiplier)) * time.Microsecond
				if sleepDuration > 0 {
					time.Sleep(sleepDuration)
				}
				currentTickTime = currentTickTime.Add(tickInterval)
			}
		}
	}
	return nil
}

func (s *MarketDataServer) parseCSVAndFilter(filepath, symbol string, startDate, endDate time.Time) ([]DailyBar, error) {

	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := bufio.NewReader(file)
	// Peek and strip UTF-8 BOM
	bom, err := r.Peek(3)
	if err == nil && len(bom) == 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		r.Discard(3)
	}

	reader := csv.NewReader(r)
	// Handling row headers
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

		if parsedDate.Before(startDate) || parsedDate.After(endDate) {
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
		// reverse append to maintain chronological order
		validDays = append([]DailyBar{bar}, validDays...)
	}

	return validDays, nil

}
