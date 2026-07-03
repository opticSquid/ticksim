package marketdataserver

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	pb "github.com/opticSquid/ticksim/gen/marketdata/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *MarketDataServer) StreamMarketData(ctx context.Context, req *pb.StreamMarketDataRequest, stream *connect.ServerStream[pb.StreamMarketDataResponse]) error {
	symbols := req.Symbols
	slog.Info("Starting real-time market data generation with dynamic resolutions", "symbols", symbols)

	startDate := req.StartDate.AsTime()
	endDate := req.EndDate.AsTime()

	barChan := make(chan DailyBar, len(symbols)*5)
	errChan := make(chan error, len(symbols))

	var wg sync.WaitGroup

	// Fan-out: Spawn a worker goroutine for each requested symbol
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			sym = strings.TrimSpace(sym)
			filepath := fmt.Sprintf("%s/01-07-2025-TO-01-07-2026-%s-ALL-N.csv", s.BaseDataPath, sym)

			if err := s.streamAndFilterCSV(ctx, filepath, sym, startDate, endDate, barChan); err != nil {
				slog.Error("worker parsing error", "symbol", sym, "error", err)
				errChan <- err
			}
		}(symbol)
	}

	go func() {
		wg.Wait()
		close(barChan)
		close(errChan)
	}()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var sequenceNumber uint64 = 1

	// Fan-In / Consumer loop
	for day := range barChan {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Establish market open and close timestamps in UTC
		marketOpen := time.Date(day.Date.Year(), day.Date.Month(), day.Date.Day(), 3, 45, 0, 0, time.UTC)
		marketClose := time.Date(day.Date.Year(), day.Date.Month(), day.Date.Day(), 10, 0, 0, 0, time.UTC)

		// Determine internal processing and response structures based on payload requirements
		var resolutionDuration time.Duration
		var returnBar bool

		switch req.Resolution {
		case pb.DataResolution_DATA_RESOLUTION_1_MIN:
			resolutionDuration = 1 * time.Minute
			returnBar = true
		case pb.DataResolution_DATA_RESOLUTION_5_MIN:
			resolutionDuration = 5 * time.Minute
			returnBar = true
		case pb.DataResolution_DATA_RESOLUTION_1_HOUR:
			resolutionDuration = 1 * time.Hour
			returnBar = true
		case pb.DataResolution_DATA_RESOLUTION_1_DAY:
			resolutionDuration = 7 * time.Hour // Complete trading session (3:45 to 10:00 UTC is 6h15m)
			returnBar = true
		default:
			resolutionDuration = 0 // Ticks do not aggregate across a duration window
			returnBar = false
		}

		// Simulated high-frequency internal tick rate (always 5 seconds)
		tickInterval := 5 * time.Second
		currentTickTime := marketOpen
		rollingPrice := day.Open

		tradesPerTick := uint32(day.NoOfTrades / 4500)
		if tradesPerTick == 0 {
			tradesPerTick = 1
		}

		// Running aggregation state variables for building dynamic bars
		var currentBarOpen, currentBarHigh, currentBarLow, currentBarClose float64
		var currentBarVolume uint64
		var runningVwapSum float64
		var currentBarStartTime time.Time
		isBarInitialized := false

		for currentTickTime.Before(marketClose) || currentTickTime.Equal(marketClose) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// 1. Compute individual tick coordinates
			var tickPrice float64
			if currentTickTime.Equal(marketOpen) {
				tickPrice = day.Open
			} else if currentTickTime.Equal(marketClose) {
				tickPrice = day.Close
			} else {
				percentageChange := (r.Float64() * 0.01) - 0.005
				rollingPrice = rollingPrice * (1.0 + percentageChange)

				if rollingPrice > day.High {
					rollingPrice = day.High
				}
				if rollingPrice < day.Low {
					rollingPrice = day.Low
				}
				tickPrice = rollingPrice
			}

			if returnBar {
				// Initialize the aggregation window tracking anchors if needed
				if !isBarInitialized {
					currentBarStartTime = currentTickTime
					currentBarOpen = tickPrice
					currentBarHigh = tickPrice
					currentBarLow = tickPrice
					currentBarVolume = 0
					runningVwapSum = 0
					isBarInitialized = true
				}

				// Aggregate tick parameters into the active candlestick metrics
				if tickPrice > currentBarHigh {
					currentBarHigh = tickPrice
				}
				if tickPrice < currentBarLow {
					currentBarLow = tickPrice
				}
				currentBarClose = tickPrice
				currentBarVolume += uint64(tradesPerTick)
				runningVwapSum += tickPrice * float64(tradesPerTick)

				// Determine if the current tick sits on or breaks a resolution time boundary
				nextTickTime := currentTickTime.Add(tickInterval)
				isPeriodEnd := nextTickTime.After(currentBarStartTime.Add(resolutionDuration)) || currentTickTime.Equal(marketClose)

				if isPeriodEnd {
					vwap := currentBarClose
					if currentBarVolume > 0 {
						vwap = runningVwapSum / float64(currentBarVolume)
					}

					resp := &pb.StreamMarketDataResponse{
						Symbol:         day.Symbol,
						SequenceNumber: sequenceNumber,
						Timestamp:      timestamppb.New(currentTickTime), // Close timestamp anchor
						Payload: &pb.StreamMarketDataResponse_Bar{
							Bar: &pb.BarData{
								Open:   currentBarOpen,
								High:   currentBarHigh,
								Low:    currentBarLow,
								Close:  currentBarClose,
								Volume: currentBarVolume,
								Vwap:   vwap,
							},
						},
					}

					if err := stream.Send(resp); err != nil {
						return connect.NewError(connect.CodeUnavailable, fmt.Errorf("failed to send bar stream: %w", err))
					}
					sequenceNumber++
					isBarInitialized = false // Reset block state for subsequent cycle processing

					// Pacing control based on the user's resolution requirements
					sleepDuration := time.Duration(float64(resolutionDuration.Milliseconds())/float64(req.PlaybackSpeedMultiplier)) * time.Microsecond
					if sleepDuration > 0 {
						time.Sleep(sleepDuration)
					}
				}

			} else {
				// Default mode: Return individual standard sub-second raw tick entities
				resp := &pb.StreamMarketDataResponse{
					Symbol:         day.Symbol,
					SequenceNumber: sequenceNumber,
					Timestamp:      timestamppb.New(currentTickTime),
					Payload: &pb.StreamMarketDataResponse_Tick{
						Tick: &pb.TickData{
							BidPrice:       tickPrice - 0.05,
							AskPrice:       tickPrice + 0.05,
							LastTradePrice: tickPrice,
							LastTradeSize:  tradesPerTick,
						},
					},
				}

				if err := stream.Send(resp); err != nil {
					return connect.NewError(connect.CodeUnavailable, fmt.Errorf("failed to send tick stream: %w", err))
				}
				sequenceNumber++

				sleepDuration := time.Duration(float64(tickInterval.Milliseconds())/float64(req.PlaybackSpeedMultiplier)) * time.Microsecond
				if sleepDuration > 0 {
					time.Sleep(sleepDuration)
				}
			}

			currentTickTime = currentTickTime.Add(tickInterval)
		}
	}

	for err := range errChan {
		if err != nil {
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("underlying concurrency data failure: %w", err))
		}
	}

	return nil
}
