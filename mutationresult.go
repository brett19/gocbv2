package gocb

type MutationResult struct {
	mt  MutationToken
	cas Cas
}

func (mr MutationResult) MutationToken() MutationToken {
	return mr.mt
}

func (mr MutationResult) Cas() Cas {
	return mr.cas
}
