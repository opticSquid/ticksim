package marketdataserver

import (
	"bufio"
	"context"
	"encoding/csv"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

func (s *MarketDataServer) streamAndFilterCSV(ctx context.Context, filepath, symbol string, startDate, endDate time.Time, out chan<- DailyBar) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	r := bufio.NewReader(file)
	bom, err := r.Peek(3)
	if err == nil && len(bom) == 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		r.Discard(3)
	}

	reader := csv.NewReader(r)
	if _, err = reader.Read(); err != nil {
		return err
	}

	var filteredDays []DailyBar

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		dateStr := strings.TrimSpace(row[2])
		parsedDate, err := time.Parse("02-Jan-2006", dateStr)
		if err != nil {
			continue
		}

		if parsedDate.Before(startDate) || parsedDate.After(endDate) {
			continue
		}

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

		filteredDays = append(filteredDays, bar)
	}

	slices.SortFunc(filteredDays, func(a, b DailyBar) int {
		if a.Date.Before(b.Date) {
			return -1
		}
		if a.Date.After(b.Date) {
			return 1
		}
		return 0
	})

	for _, bar := range filteredDays {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- bar:
		}
	}
	return nil
}
