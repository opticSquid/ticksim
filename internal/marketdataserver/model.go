package marketdataserver

import (
	"time"

	"github.com/opticSquid/ticksim/gen/marketdata/v1/marketdataconnect"
)

// Internal structure representing a row in your specific Indian NSE CSV format
type DailyBar struct {
	Symbol       string
	Date         time.Time // Normalized to the target day
	Open         float64
	High         float64
	Low          float64
	Close        float64
	LastPrice    float64
	Volume       uint64
	NoOfTrades   uint64
	AveragePrice float64
}

type MarketDataServer struct {
	// Optional embedding for forward-compatibility
	marketdataconnect.UnimplementedMarketDataServiceHandler
	BaseDataPath string
}
