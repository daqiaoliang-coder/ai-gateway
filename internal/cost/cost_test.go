package cost

import "testing"

func testTable(t *testing.T) *Table {
	t.Helper()
	table, err := NewTable([]Price{
		{
			Model: "gpt-4.1-mini", Provider: "openai-primary", Currency: "USD", Unit: UnitToken,
			Version: "2026-01", InputRate: 1, OutputRate: 4, RequestRate: 0,
		},
		{
			Model: "backup-chat-model", Provider: "compatible-backup", Currency: "USD", Unit: UnitMillionToken,
			Version: "2026-01", InputRate: 400_000, OutputRate: 1_600_000, RequestRate: 0,
		},
	})
	if err != nil {
		t.Fatalf("build table: %v", err)
	}
	return table
}

func TestSettleAppliesTokenUnitPrices(t *testing.T) {
	table := testTable(t)
	settlement, err := table.Settle("openai-primary", "gpt-4.1-mini",
		Usage{InputTokens: 100, OutputTokens: 50, Reported: true},
		true, Estimate, nil, 0, 0)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if settlement.Total != 100*1+50*4 {
		t.Fatalf("total = %d, want %d", settlement.Total, 100*1+50*4)
	}
	if settlement.Status != StatusOK || !settlement.PriceFound {
		t.Fatalf("status = %s found = %v, want ok/true", settlement.Status, settlement.PriceFound)
	}
	if settlement.Currency != "USD" || settlement.Version != "2026-01" {
		t.Fatalf("currency=%s version=%s, want USD/2026-01", settlement.Currency, settlement.Version)
	}
}

func TestSettleAppliesPerMillionPrices(t *testing.T) {
	table := testTable(t)
	// 1,000,000 input tokens at 400,000 micros per million.
	settlement, err := table.Settle("compatible-backup", "backup-chat-model",
		Usage{InputTokens: 1_000_000, OutputTokens: 0, Reported: true},
		true, Estimate, nil, 0, 0)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if settlement.Total != 400_000 {
		t.Fatalf("total = %d, want 400000 micros ($0.40)", settlement.Total)
	}
}

// Usage the upstream never reported must be flagged, never silently assumed to
// be zero, because a zero is indistinguishable from a free request later.
func TestMissingUsagePolicies(t *testing.T) {
	table := testTable(t)

	estimated, err := table.Settle("openai-primary", "gpt-4.1-mini", Usage{}, true, Estimate, CharEstimator{CharsPerToken: 4}, 400, 800)
	if err != nil {
		t.Fatalf("estimate settle: %v", err)
	}
	if estimated.Status != StatusEstimated {
		t.Fatalf("status = %s, want estimated", estimated.Status)
	}
	if estimated.Usage.InputTokens != 100 || estimated.Usage.OutputTokens != 200 {
		t.Fatalf("estimated usage = %+v, want 100/200", estimated.Usage)
	}

	zeroed, err := table.Settle("openai-primary", "gpt-4.1-mini", Usage{}, true, AllowZero, nil, 400, 800)
	if err != nil {
		t.Fatalf("allow_zero settle: %v", err)
	}
	if zeroed.Status != StatusMissing {
		t.Fatalf("status = %s, want missing", zeroed.Status)
	}
	if zeroed.Total != 0 {
		t.Fatalf("total = %d, want 0 with no usage", zeroed.Total)
	}

	if _, err := table.Settle("openai-primary", "gpt-4.1-mini", Usage{}, true, Reject, nil, 400, 800); err == nil {
		t.Fatal("reject policy must surface missing usage as an error")
	}
}

func TestUnknownPriceIsReportedNotGuessed(t *testing.T) {
	table := testTable(t)
	settlement, err := table.Settle("openai-primary", "some-other-model",
		Usage{InputTokens: 10, OutputTokens: 10, Reported: true}, true, Estimate, nil, 0, 0)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if settlement.PriceFound {
		t.Fatal("price must be reported as missing")
	}
	if settlement.Status != StatusMissing {
		t.Fatalf("status = %s, want missing", settlement.Status)
	}
	if settlement.Total != 0 {
		t.Fatalf("total = %d, want 0 when no price is known", settlement.Total)
	}
}

func TestIncompleteRequestIsPartial(t *testing.T) {
	table := testTable(t)
	settlement, err := table.Settle("openai-primary", "gpt-4.1-mini",
		Usage{InputTokens: 10, OutputTokens: 10, Reported: true}, false, Estimate, nil, 0, 0)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if settlement.Status != StatusPartial {
		t.Fatalf("status = %s, want partial", settlement.Status)
	}
	if settlement.Total == 0 {
		t.Fatal("partial output still costs money and must be charged")
	}
}

func TestTableValidation(t *testing.T) {
	if _, err := NewTable([]Price{{Model: "m", Provider: "p", Currency: "usd", Unit: UnitToken, Version: "v1"}}); err == nil {
		t.Fatal("lowercase currency must be rejected")
	}
	if _, err := NewTable([]Price{{Model: "m", Provider: "p", Currency: "USD", Unit: UnitToken}}); err == nil {
		t.Fatal("a price without a version must be rejected")
	}
	if _, err := NewTable([]Price{
		{Model: "m", Provider: "p", Currency: "USD", Unit: UnitToken, Version: "v1"},
		{Model: "m", Provider: "p", Currency: "USD", Unit: UnitToken, Version: "v1"},
	}); err == nil {
		t.Fatal("duplicate price versions must be rejected")
	}
}

func TestPricesAreVersioned(t *testing.T) {
	table := testTable(t)
	price, found := table.Lookup("openai-primary", "gpt-4.1-mini")
	if !found {
		t.Fatal("expected to find the configured price")
	}
	if price.Version == "" {
		t.Fatal("every price must carry a version")
	}
	if _, found := table.Lookup("openai-primary", "unknown"); found {
		t.Fatal("unknown models must not resolve")
	}
}
