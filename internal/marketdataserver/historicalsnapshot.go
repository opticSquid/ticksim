package marketdataserver

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"connectrpc.com/connect"
	pb "github.com/opticSquid/ticksim/gen/marketdata/v1"
)

func (s *MarketDataServer) GetHistoricalSnapshot(
	ctx context.Context,
	req *pb.GetHistoricalSnapshotRequest,
) (*pb.GetHistoricalSnapshotResponse, error) {
	symbol := strings.TrimSpace(req.Symbol)
	slog.Info("starting historical snapshot", "symbol", symbol, "start time", req, req.StartTime.AsTime(), "end time", req.EndTime.AsTime())
	filepath := fmt.Sprintf("%s/01-07-2025-TO-01-07-2026-%s-ALL-N.csv", s.BaseDataPath, symbol)
	barChan := make(chan DailyBar)

	if err := s.streamAndFilterCSV(ctx, filepath, symbol, req.StartTime.AsTime(), req.EndTime.AsTime(), barChan); err != nil {
		slog.Error("worker parsing error", "symbol", symbol, "error", err)
		return nil, connect.NewError(connect.CodeDataLoss, fmt.Errorf("data could not be parded for %s", symbol))
	}
	// Determine internal processing and response structures based on payload requirements
	var resolutionDuration time.Duration
	switch req.Resolution {
	case pb.DataResolution_DATA_RESOLUTION_1_MIN:
		resolutionDuration = 1 * time.Minute
	case pb.DataResolution_DATA_RESOLUTION_5_MIN:
		resolutionDuration = 5 * time.Minute
	case pb.DataResolution_DATA_RESOLUTION_1_HOUR:
		resolutionDuration = 1 * time.Hour
	case pb.DataResolution_DATA_RESOLUTION_1_DAY:
		resolutionDuration = 7 * time.Hour // Complete trading session (3:45 to 10:00 UTC is 6h15m)
	default:
		resolutionDuration = 5 * time.Second
	}
	for day := range barChan {
		marketOpen := time.Date(day.Date.Year(), day.Date.Month(), day.Date.Day(), 3, 45, 0, 0, time.UTC)
		marketClose := time.Date(day.Date.Year(), day.Date.Month(), day.Date.Day(), 10, 0, 0, 0, time.UTC)
		currentTickTime := marketOpen
	}
}
