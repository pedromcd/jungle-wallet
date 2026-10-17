package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
)

// Money is an immutable value object: an int64 amount in minor units (cents,
// fixed scale of 2) plus an ISO 4217 currency. Range: |amount| < 2^63 cents
// (about 92 quadrillion units). Every operation checks for overflow. No
// float32/float64 is used anywhere: parsing, arithmetic, JSON or storage.
type Money struct {
	minor    int64
	currency string
}

// Only 2-decimal ISO 4217 currencies are supported (the scale is fixed at 2).
var supportedCurrencies = map[string]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "ARS": {}, "MXN": {},
	"CAD": {}, "AUD": {}, "CHF": {}, "COP": {}, "PEN": {}, "UYU": {},
}

var amountRe = regexp.MustCompile(`^(0|[1-9][0-9]*)\.([0-9]{2})$`)

func validateCurrency(c string) error {
	if _, ok := supportedCurrencies[c]; !ok {
		return fmt.Errorf("%w: %q", ErrInvalidCurrency, c)
	}
	return nil
}

// NewMoney builds Money from minor units (may be negative: internal diffs).
func NewMoney(minor int64, currency string) (Money, error) {
	if err := validateCurrency(currency); err != nil {
		return Money{}, err
	}
	if minor == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: minor, currency: currency}, nil
}

// Zero returns the zero amount of a currency.
func Zero(currency string) (Money, error) { return NewMoney(0, currency) }

// ParseMoney parses an external decimal string ("25.00"). It accepts only a
// non-negative amount with exactly two decimals: no sign, no exponent, no
// NaN/Infinity, no extra scale. Nothing is ever rounded.
func ParseMoney(amount, currency string) (Money, error) {
	if err := validateCurrency(currency); err != nil {
		return Money{}, err
	}
	m := amountRe.FindStringSubmatch(amount)
	if m == nil {
		return Money{}, fmt.Errorf("%w: %q (expected a non-negative decimal with exactly 2 places)", ErrInvalidAmount, amount)
	}
	whole, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return Money{}, ErrMoneyOverflow
	}
	cents, _ := strconv.ParseInt(m[2], 10, 64)
	if whole > (math.MaxInt64-cents)/100 {
		return Money{}, ErrMoneyOverflow
	}
	return NewMoney(whole*100+cents, currency)
}

func (m Money) Currency() string  { return m.currency }
func (m Money) Minor() int64      { return m.minor }
func (m Money) Initialized() bool { return m.currency != "" }
func (m Money) IsZero() bool      { return m.currency != "" && m.minor == 0 }
func (m Money) IsPositive() bool  { return m.currency != "" && m.minor > 0 }
func (m Money) IsNegative() bool  { return m.currency != "" && m.minor < 0 }

func (m Money) compat(o Money) error {
	if m.currency == "" || o.currency == "" {
		return ErrUninitializedMoney
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add returns m+o, failing on currency mismatch or overflow.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compat(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor <= math.MinInt64-o.minor) {
		return Money{}, ErrMoneyOverflow
	}
	return NewMoney(m.minor+o.minor, m.currency)
}

// Neg returns -m.
func (m Money) Neg() (Money, error) {
	if m.currency == "" {
		return Money{}, ErrUninitializedMoney
	}
	return NewMoney(-m.minor, m.currency) // MinInt64 is unrepresentable by construction
}

// Sub returns m-o.
func (m Money) Sub(o Money) (Money, error) {
	n, err := o.Neg()
	if err != nil {
		return Money{}, err
	}
	return m.Add(n)
}

// Cmp returns -1, 0 or 1; it fails for mixed or uninitialized currencies.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compat(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

// Equal is true only for the same currency and the same amount.
func (m Money) Equal(o Money) bool {
	c, err := m.Cmp(o)
	return err == nil && c == 0
}

// Amount renders the canonical decimal string ("25.00"; "-1.05" for diffs).
func (m Money) Amount() string {
	abs := m.minor
	sign := ""
	if abs < 0 {
		abs = -abs
		sign = "-"
	}
	return fmt.Sprintf("%s%d.%02d", sign, abs/100, abs%100)
}

func (m Money) String() string { return m.Amount() + " " + m.currency }

type moneyJSON struct {
	Amount   *string `json:"amount"`
	Currency *string `json:"currency"`
}

// MarshalJSON emits {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if m.currency == "" {
		return nil, ErrUninitializedMoney
	}
	a, c := m.Amount(), m.currency
	return json.Marshal(moneyJSON{Amount: &a, Currency: &c})
}

// UnmarshalJSON is strict: amount must be a JSON *string* (JSON numbers, which
// would travel through float decoders, are rejected), as are unknown fields.
func (m *Money) UnmarshalJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var raw moneyJSON
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	if raw.Amount == nil || raw.Currency == nil {
		return fmt.Errorf("%w: amount and currency are required", ErrInvalidAmount)
	}
	v, err := ParseMoney(*raw.Amount, *raw.Currency)
	if err != nil {
		return err
	}
	*m = v
	return nil
}
