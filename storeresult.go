package gocb

// StoreResult is the return type of any store related operations. It contains Cas and mutation tokens.
type StoreResult struct {
	mt  MutationToken
	cas Cas
}

// MutationToken returns the mutation token belonging to an operation.
func (mr StoreResult) MutationToken() MutationToken {
	return mr.mt
}

// Cas returns the Cas value for a document following an operation.
func (mr StoreResult) Cas() Cas {
	return mr.cas
}
