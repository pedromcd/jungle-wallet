package domain

// Verdict of evaluating an operation against its referenced transaction.
type Verdict int

const (
	// Proceed: the reference is valid, the movement may be applied.
	Proceed Verdict = iota
	// Wait: the reference is missing or not finished yet; keep PENDING_REFERENCE.
	Wait
	// RejectVerdict: definitive rejection with a failure code.
	RejectVerdict
)

// ReferenceDecision is the outcome of EvaluateReference.
type ReferenceDecision struct {
	Verdict Verdict
	Code    FailureCode // set only for RejectVerdict
}

func reject(code FailureCode) ReferenceDecision {
	return ReferenceDecision{Verdict: RejectVerdict, Code: code}
}

// EvaluateReference applies the reference rules of WIN (optional BET
// reference), REFUND (BET only) and ROLLBACK (BET, WIN or REFUND). ref is nil
// when it has not arrived. alreadyReversed tells whether ref already has a
// successful REFUND/ROLLBACK: a transaction can be reversed at most once, so a
// REFUND and a ROLLBACK can never both succeed over the same BET.
func EvaluateReference(op, ref *WagerTransaction, alreadyReversed bool) ReferenceDecision {
	if ref == nil {
		return ReferenceDecision{Verdict: Wait}
	}
	switch ref.Status() {
	case StatusPending, StatusPendingReference:
		return ReferenceDecision{Verdict: Wait}
	case StatusRejected, StatusFailed:
		return reject(FailReferenceNotSuccessful)
	}
	// ref is PROCESSED from here on.
	switch op.Kind() {
	case KindRefund, KindWin:
		if ref.Kind() != KindBet {
			return reject(FailReferenceInvalidKind)
		}
	case KindRollback:
		if ref.Kind() != KindBet && ref.Kind() != KindWin && ref.Kind() != KindRefund {
			return reject(FailReferenceInvalidKind)
		}
	default:
		return reject(FailReferenceInvalidKind)
	}
	if ref.ProviderID() != op.ProviderID() || ref.PlayerID() != op.PlayerID() ||
		ref.WalletID() != op.WalletID() || ref.RoundID() != op.RoundID() ||
		ref.Money().Currency() != op.Money().Currency() {
		return reject(FailReferenceMismatch)
	}
	if op.Kind() != KindWin && !ref.Money().Equal(op.Money()) {
		return reject(FailReferenceAmountMismatch) // reversals are always full
	}
	if alreadyReversed && (op.Kind() == KindRefund || op.Kind() == KindRollback) {
		return reject(FailReferenceAlreadyReversed)
	}
	return ReferenceDecision{Verdict: Proceed}
}

// MovementDirection returns how an operation moves the wallet. ref is only
// needed for ROLLBACK, which is the opposite movement of the original one.
func MovementDirection(op, ref *WagerTransaction) Direction {
	switch op.Kind() {
	case KindBet:
		return Debit
	case KindWin, KindRefund:
		return Credit
	case KindRollback:
		if ref != nil && ref.Kind() == KindBet {
			return Credit // undoing a debit
		}
		return Debit // undoing a WIN or a REFUND credit
	}
	return Credit
}
