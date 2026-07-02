package main

import "time"

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
	baseDataPath string
}
