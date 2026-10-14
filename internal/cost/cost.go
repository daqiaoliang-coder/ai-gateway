// Package cost turns reported usage into money using a versioned price table.
//
// Accounting must be exact while enforcement may be approximate: this package is
// on the exact side of that line. Every computation records how trustworthy the
// input was, so downstream reconciliation can tell a real zero from a missing
// usage field.
package cost

import (
	"fmt"
	"strings"
	"time"
)

// Micros is one millionth of a currency unit. The gateway keeps money as int64
// micro units so that repeated additions stay exact and never drift.
type Micros int64

// MicrosPerUnit is the number of micro units in one whole currency unit.
const MicrosPerUnit = 1_000_000

// Unit describes what a price is charged per.
type Unit string

const (
	// UnitToken charges per single token.
	UnitToken Unit = "token"
	// UnitMillionToken charges per one million tokens.
	UnitMillionToken Unit = "1m_tokens"
	// UnitRequest charges a flat amount per request.
	UnitRequest Unit = "request"
)

// Price is one billable rate. Prices are versioned because a provider can change
// rates, and historical usage must still settle against the version that was in
// effect when it happened.
type Price struct {
	Model       string    `json:"model"`
	Provider    string    `json:"provider"`
	Currency    string    `json:"currency"`
	Unit        Unit      `json:"unit"`
	Version     string    `json:"version"`
	InputRate   Micros    `json:"input_rate"`
	OutputRate  Micros    `json:"output_rate"`
	RequestRate Micros    `json:"request_rate"`
	EffectiveAt time.Time `json:"effective_at"`
}

// Validate rejects prices the table cannot apply unambiguously.
func (p Price) Validate() error {
	if p.Model == "" || p.Provider == "" {
		return fmt.Errorf("price requires model and provider")
	}
	if len(p.Currency) != 3 || strings.ToUpper(p.Currency) != p.Currency {
		return fmt.Errorf("price %s/%s currency must be an uppercase ISO 4217 code", p.Provider, p.Model)
	}
	switch p.Unit {
	case UnitToken, UnitMillionToken, UnitRequest:
	default:
		return fmt.Errorf("price %s/%s has unknown unit %q", p.Provider, p.Model, p.Unit)
	}
	if p.Version == "" {
		return fmt.Errorf("price %s/%s requires a version", p.Provider, p.Model)
	}
	if p.InputRate < 0 || p.OutputRate < 0 || p.RequestRate < 0 {
		return fmt.Errorf("price %s/%s rates must not be negative", p.Provider, p.Model)
	}
	return nil
}

// Usage is what the upstream reported for a single attempt.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
	// Reported is false when the upstream omitted usage entirely.
	Reported bool
}

// Line is the settled result of applying one price to one usage.
type Line struct {
	Currency    string `json:"currency"`
	Unit        Unit   `json:"unit"`
	Version     string `json:"version"`
	InputCost   Micros `json:"input_cost_micros"`
	OutputCost  Micros `json:"output_cost_micros"`
	RequestCost Micros `json:"request_cost_micros"`
	Total       Micros `json:"total_micros"`
}

// Table holds every known price keyed by provider and model.
type Table struct {
	prices map[string]Price
}

// Key identifies a price entry.
func Key(provider, model string) string { return provider + "\x00" + model }

// NewTable builds a table and validates every entry.
func NewTable(prices []Price) (*Table, error) {
	table := &Table{prices: make(map[string]Price, len(prices))}
	for _, price := range prices {
		if err := price.Validate(); err != nil {
			return nil, err
		}
		if previous, exists := table.prices[Key(price.Provider, price.Model)]; exists && previous.Version == price.Version {
			return nil, fmt.Errorf("duplicate price version %s for %s/%s", price.Version, price.Provider, price.Model)
		}
		table.prices[Key(price.Provider, price.Model)] = price
	}
	return table, nil
}

// Lookup returns the price for a provider and model.
func (t *Table) Lookup(provider, model string) (Price, bool) {
	if t == nil {
		return Price{}, false
	}
	price, ok := t.prices[Key(provider, model)]
	return price, ok
}

// Status reports how much a usage reading can be trusted. It travels with every
// settled cost so reconciliation never assumes a missing field means zero.
type Status string

const (
	// StatusOK means usage was reported and a price was found.
	StatusOK Status = "ok"
	// StatusMissing means usage was reported but no price exists, so the cost
	// cannot be computed. tokens are still known.
	StatusMissing Status = "missing"
	// StatusEstimated means usage was absent and had to be estimated.
	StatusEstimated Status = "estimated"
	// StatusPartial means the request did not complete, so usage covers only part
	// of the response.
	StatusPartial Status = "partial"
)

// EstimatePolicy decides what to do when the upstream reports no usage.
type EstimatePolicy string

const (
	// Estimate derive tokens from what is known and mark the result estimated.
	Estimate EstimatePolicy = "estimate"
	// Reject fails the settlement so the operator learns about it.
	Reject EstimatePolicy = "reject"
	// AllowZero records zero tokens without pretending to know better.
	AllowZero EstimatePolicy = "allow_zero"
)

// Estimator produces a token count when the upstream did not report one.
type Estimator interface {
	EstimateTokens(inputBytes, outputBytes int64) Usage
}

// CharEstimator approximates tokens from body size. It is deliberately coarse:
// an estimate exists to keep budgets moving, not to produce a bill.
type CharEstimator struct {
	CharsPerToken int
}

// EstimateTokens converts bytes into a plausible token count.
func (e CharEstimator) EstimateTokens(inputBytes, outputBytes int64) Usage {
	perToken := e.CharsPerToken
	if perToken <= 0 {
		perToken = 4
	}
	return Usage{
		InputTokens:  inputBytes / int64(perToken),
		OutputTokens: outputBytes / int64(perToken),
		Reported:     false,
	}
}

// Settlement is the result of costing one completed request.
type Settlement struct {
	Line
	Usage      Usage  `json:"usage"`
	Status     Status `json:"reconciliation_status"`
	PriceFound bool   `json:"price_found"`
}

// Settle applies the price table to a usage reading. It never returns an error
// for missing data; it records the reason instead, because accounting must stay
// complete even when the input is not.
func (t *Table) Settle(provider, model string, usage Usage, complete bool, policy EstimatePolicy, estimator Estimator, inputBytes, outputBytes int64) (Settlement, error) {
	settlement := Settlement{Usage: usage}

	switch {
	case usage.Reported:
		settlement.Status = StatusOK
	case policy == Reject:
		return settlement, fmt.Errorf("usage missing for %s/%s and policy is reject", provider, model)
	case policy == AllowZero:
		// Nothing is known, and pretending otherwise would corrupt totals, so the
		// tokens stay zero and the reading is flagged instead.
		settlement.Usage = Usage{Reported: false}
		settlement.Status = StatusMissing
	default:
		if estimator == nil {
			estimator = CharEstimator{CharsPerToken: 4}
		}
		settlement.Usage = estimator.EstimateTokens(inputBytes, outputBytes)
		settlement.Status = StatusEstimated
	}
	if !complete && settlement.Status == StatusOK {
		settlement.Status = StatusPartial
	}

	price, found := t.Lookup(provider, model)
	if !found {
		settlement.PriceFound = false
		settlement.Status = StatusMissing
		return settlement, nil
	}
	settlement.PriceFound = true
	settlement.Currency = price.Currency
	settlement.Unit = price.Unit
	settlement.Version = price.Version
	settlement.InputCost = price.Cost(false, settlement.Usage.InputTokens)
	settlement.OutputCost = price.Cost(true, settlement.Usage.OutputTokens)
	settlement.RequestCost = price.RequestRate
	settlement.Total = settlement.InputCost + settlement.OutputCost + settlement.RequestCost
	return settlement, nil
}

// Cost applies a rate to a token count according to the price unit.
func (p Price) Cost(output bool, tokens int64) Micros {
	if tokens <= 0 {
		return 0
	}
	rate := p.InputRate
	if output {
		rate = p.OutputRate
	}
	switch p.Unit {
	case UnitRequest:
		return 0
	case UnitMillionToken:
		// rate * tokens / 1e6, computed in two steps to keep precision.
		return Micros((int64(rate) * tokens) / 1_000_000)
	default:
		return Micros(int64(rate) * tokens)
	}
}
