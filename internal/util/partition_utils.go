package util

import (
	"strings"

	"github.com/csimplestring/delta-go/types"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/repeale/fp-go"
	"github.com/samber/mo"
)

func SplitMetadataAndDataPredicates(condition types.Expression,
	partitionColumns []string) (mo.Option[types.Expression], mo.Option[types.Expression]) {

	var metadataPredicates []types.Expression
	var dataPredicates []types.Expression
	for _, p := range SplitConjunctivePredicates(condition) {
		if IsPredicateMetadataOnly(p, partitionColumns) {
			metadataPredicates = append(metadataPredicates, p)
		} else {
			dataPredicates = append(dataPredicates, p)
		}
	}

	var metadataConjunction mo.Option[types.Expression]
	if len(metadataPredicates) == 0 {
		metadataConjunction = mo.None[types.Expression]()
	} else {
		reducer := func(acc types.Expression, curr types.Expression) types.Expression {
			if acc == nil {
				// return the first expression
				return curr
			}
			return types.NewAnd(acc, curr)
		}
		metadataConjunction = mo.Some(fp.Reduce(reducer, nil)(metadataPredicates))
	}

	var dataConjunction mo.Option[types.Expression]
	if len(dataPredicates) == 0 {
		dataConjunction = mo.None[types.Expression]()
	} else {
		reducer := func(acc types.Expression, curr types.Expression) types.Expression {
			if acc == nil {
				// return the first expression
				return curr
			}
			return types.NewAnd(acc, curr)
		}
		dataConjunction = mo.Some(fp.Reduce(reducer, nil)(dataPredicates))
	}

	return metadataConjunction, dataConjunction
}

func SplitConjunctivePredicates(condition types.Expression) []types.Expression {
	switch v := condition.(type) {
	case *types.And:
		return append(SplitConjunctivePredicates(v.Left), SplitConjunctivePredicates(v.Right)...)
	default:
		return []types.Expression{condition}
	}
}

func IsPredicateMetadataOnly(condition types.Expression, partitionColumns []string) bool {
	lowercasePartCols := mapset.NewSet(fp.Map(func(v string) string { return strings.ToLower(v) })(partitionColumns)...)

	columns := condition.References()

	return lowercasePartCols.Contains(columns...)
}

func MaxInt64(x int64, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func MinInt64(x int64, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
