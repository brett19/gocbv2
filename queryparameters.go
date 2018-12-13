package gocb

type QueryParameters struct {
	positionalParams []interface{}
	namedParams      map[string]interface{}
}

// NewQueryPositionalParameters creates a set of positional parameters for use in N1ql querying.
func NewQueryPositionalParameters(params []interface{}) *QueryParameters {
	qp := &QueryParameters{
		positionalParams: params,
	}
	return qp
}

// NewQueryNamedParameters creates a set of named parameters for use in N1ql querying.
func (nq *QueryParameters) NewQueryNamedParameters(params map[string]interface{}) *QueryParameters {
	qp := &QueryParameters{
		namedParams: params,
	}
	return qp
}
