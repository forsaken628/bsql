package bsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelect_Build(t *testing.T) {
	type outStruct struct {
		cond string
		vals []interface{}
	}
	var data = []struct {
		in  Select
		out outStruct
	}{
		{
			in: Select{
				Table:  Raw("tb"),
				Where:  Raw("age > ?", 23),
				Fields: []string{"count(*) as total"},
			},
			out: outStruct{
				cond: "SELECT count(*) as total FROM tb WHERE age > ?",
				vals: []interface{}{23},
			},
		},
		{
			in: Select{
				Fields:  []string{"name", "count(price) as total"},
				Table:   Raw("tb"),
				Where:   Raw("age>?", 23),
				GroupBy: []string{"name"},
				Having: SecAND{
					Raw("total >= ?", 1000),
					Raw("total < ?", 50000),
				},
			},
			out: outStruct{
				cond: "SELECT name,count(price) as total FROM tb WHERE age>? GROUP BY name HAVING (total >= ? AND total < ?)",
				vals: []interface{}{23, 1000, 50000},
			},
		},
		{
			in: Select{
				Table:  Raw("tb"),
				Fields: []string{"name", "count(price) as total"},
				Having: SecAND{
					Raw("total >= ?", 1000),
					Raw("total < ?", 50000),
				},
				GroupBy: []string{"name"},
			},
			out: outStruct{
				cond: "SELECT name,count(price) as total FROM tb GROUP BY name HAVING (total >= ? AND total < ?)",
				vals: []interface{}{1000, 50000},
			},
		},
		{
			in: Select{
				Table:  Raw("tb"),
				Fields: []string{"name", "age"},
				Where:  MakeIn("age", []interface{}{1, 2, 3}),
			},
			out: outStruct{
				cond: "SELECT name,age FROM tb WHERE age IN (?,?,?)",
				vals: []interface{}{1, 2, 3},
			},
		},
		{
			in: Select{
				Fields: []string{"a"},
				Table:  Raw("tab"),
				Where: SecOR{
					Raw("c1=1"),
					Raw("c2=?", 2),
					SecAND{
						Raw("c3=?", 3),
						Raw("c4=?", 4),
						MakeIn("c5", []interface{}{5, 6, 7, 8}),
					},
				},
			},
			out: outStruct{
				cond: "SELECT a FROM tab WHERE (c1=1 OR c2=? OR (c3=? AND c4=? AND c5 IN (?,?,?,?)))",
				vals: []interface{}{2, 3, 4, 5, 6, 7, 8},
			},
		},
		{
			in: Select{
				Table: MakeAlias(Select{
					Fields: []string{"a"},
					Table:  Raw("tab"),
					Where: SecOR{
						Raw("c1=1"),
						Raw("c2=?", 2),
						SecAND{
							Raw("c3=?", 3),
							Raw("c4=?", 4),
							MakeIn("c5", []interface{}{5, 6, 7, 8}),
						},
					},
				}, "t1"),
				Where: SecAND{
					Raw("c6 != ?", "c6"),
				},
				OrderBy: []string{"id desc"},
				Limit:   []uint{3},
			},
			out: outStruct{
				cond: "SELECT * FROM (SELECT a FROM tab WHERE (c1=1 OR c2=? OR (c3=? AND c4=? AND c5 IN (?,?,?,?)))) AS t1 WHERE (c6 != ?) ORDER BY id desc LIMIT ?",
				vals: []interface{}{2, 3, 4, 5, 6, 7, 8, "c6", uint(3)},
			},
		},
		{
			in: Select{
				Fields: []string{"name", "max(age)"},
				Table:  Raw("tab"),
				Where: SecAND{
					Raw("c6 != ?", "c6"),
				},
				GroupBy: []string{"name"},
				Having:  Raw("age > ?", 10),
				Limit:   []uint{0, 10},
			},
			out: outStruct{
				cond: "SELECT name,max(age) FROM tab WHERE (c6 != ?) GROUP BY name HAVING age > ? LIMIT ?,?",
				vals: []interface{}{"c6", 10, uint(0), uint(10)},
			},
		},
		{
			in: Select{
				Distinct: true,
				Table:    Raw("tb"),
				Where:    Raw("age > ?", 23),
				Fields:   []string{"col"},
			},
			out: outStruct{
				cond: "SELECT DISTINCT col FROM tb WHERE age > ?",
				vals: []interface{}{23},
			},
		},
	}

	ass := assert.New(t)
	for _, tc := range data {
		q, a := tc.in.Build()
		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func TestInsert_Build(t *testing.T) {
	ass := assert.New(t)
	type inStruct struct {
		table  string
		cols   []string
		values [][]interface{}
	}
	type outStruct struct {
		cond string
		vals []interface{}
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				cols:  []string{"age", "foo"},
				values: [][]interface{}{
					{23, "bar"},
				},
			},
			out: outStruct{
				cond: "INSERT INTO tb (age,foo) VALUES (?,?)",
				vals: []interface{}{23, "bar"},
			},
		},
		{
			in: inStruct{
				table: "tab",
				cols:  nil,
				values: [][]interface{}{
					{"a", 1},
					{"b", 2},
				},
			},
			out: outStruct{
				cond: "INSERT INTO tab VALUES (?,?),(?,?)",
				vals: []interface{}{"a", 1, "b", 2},
			},
		},
	}
	for _, tc := range data {
		v, err := MakeValues(tc.in.cols, tc.in.values)
		ass.NoError(err)

		q, a := Insert{
			Table: Raw(tc.in.table),
			Value: v,
		}.Build()
		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func TestUpdate_Build(t *testing.T) {
	type outStruct struct {
		cond string
		vals []interface{}
	}
	var data = []struct {
		in  Update
		out outStruct
	}{
		{
			in: Update{
				Table: Raw("tb"),
				Set: MakeSetSort(map[string]interface{}{
					"district": 50,
					"score":    "010",
				}),
				Where: SecAND{
					Raw("foo = ?", "bar"),
					Raw("age >= ?", 23),
					MakeIn("sex", []interface{}{"male", "female"}),
				},
			},
			out: outStruct{
				cond: "UPDATE tb SET district=?,score=? WHERE (foo = ? AND age >= ? AND sex IN (?,?))",
				vals: []interface{}{50, "010", "bar", 23, "male", "female"},
			},
		},
		{
			in: Update{
				Table: Raw("tab"),
				Set:   Raw("a = a + ?", 1),
				Where: Raw("id = ?", 50),
			},
			out: outStruct{
				cond: "UPDATE tab SET a = a + ? WHERE id = ?",
				vals: []interface{}{1, 50},
			},
		},
	}

	ass := assert.New(t)
	for _, tc := range data {
		q, a := tc.in.Build()
		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func TestDelete_Build(t *testing.T) {
	type outStruct struct {
		cond string
		vals []interface{}
	}
	var data = []struct {
		in  Delete
		out outStruct
	}{
		{
			in: Delete{
				Table: Raw("tb"),
				Where: SecAND{
					MakeIn("hobby", []interface{}{"soccer", "basketball", "tenis"}),
					MakeIn("sex", []interface{}{"male", "female"}),
					Raw("age >= ?", 21),
				},
			},
			out: outStruct{
				cond: "DELETE FROM tb WHERE (hobby IN (?,?,?) AND sex IN (?,?) AND age >= ?)",
				vals: []interface{}{"soccer", "basketball", "tenis", "male", "female", 21},
			},
		},
		{
			in: Delete{
				Table: Raw("tab"),
				Where: Raw("id = ?", 50),
			},
			out: outStruct{
				cond: "DELETE FROM tab WHERE id = ?",
				vals: []interface{}{50},
			},
		},
	}

	ass := assert.New(t)
	for _, tc := range data {
		q, a := tc.in.Build()
		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func TestSecCase(t *testing.T) {
	type outStruct struct {
		cond string
		vals []interface{}
	}
	var data = []struct {
		in  SecCase
		out outStruct
	}{
		{
			in: SecCase{
				When: [][2]Builder{
					{
						Raw("1"), Raw("'A'"),
					},
				},
			},
			out: outStruct{
				cond: "CASE WHEN 1 THEN 'A' END",
				vals: []interface{}{},
			},
		},
		{
			in: SecCase{
				Case: Raw("col"),
				When: [][2]Builder{
					{Raw("1"), Raw("'A'")},
					{Raw("2"), Raw("'B'")},
				},
				Else: Raw("null"),
			},
			out: outStruct{
				cond: "CASE col WHEN 1 THEN 'A' WHEN 2 THEN 'B' ELSE null END",
				vals: []interface{}{},
			},
		},
		{
			in: SecCase{
				Case: Raw("?", "col"),
				When: [][2]Builder{
					{Raw("?", "when1"), Raw("?", "then1")},
					{Raw("?", "when2"), Raw("?", "then2")},
				},
				Else: Raw("?", "else"),
			},
			out: outStruct{
				cond: "CASE ? WHEN ? THEN ? WHEN ? THEN ? ELSE ? END",
				vals: []interface{}{"col", "when1", "then1", "when2", "then2", "else"},
			},
		},
	}

	ass := assert.New(t)
	for _, tc := range data {
		q, a := tc.in.Build()
		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func TestMakeJoin(t *testing.T) {
	type outStruct struct {
		cond string
		vals []interface{}
	}

	var data = []struct {
		in  Builder
		out outStruct
	}{
		{MakeJoin(InnerJoin,
			MakeAlias(Raw("t1"), "t1"),
			MakeAlias(Raw("t2"), "t2"),
			Raw("t1.id = t2.id"),
		), outStruct{
			cond: "t1 AS t1 JOIN t2 AS t2 ON t1.id = t2.id",
			vals: nil,
		}},
		{MakeJoin(LeftJoin,
			MakeAlias(Raw("t1"), "t1"),
			MakeAlias(Raw("t2"), "t2"),
			Raw("t1.id = t2.id"),
		), outStruct{
			cond: "t1 AS t1 LEFT JOIN t2 AS t2 ON t1.id = t2.id",
			vals: nil,
		}},
		{MakeJoin(RightJoin,
			MakeAlias(Raw("t1"), "t1"),
			MakeAlias(Raw("t2"), "t2"),
			Raw("t1.id = t2.id"),
		), outStruct{
			cond: "t1 AS t1 RIGHT JOIN t2 AS t2 ON t1.id = t2.id",
			vals: nil,
		}},
		{MakeJoin(CrossJoin,
			MakeAlias(Raw("t1"), "t1"),
			MakeAlias(Raw("t2"), "t2"),
			Raw("t1.id = t2.id"),
		), outStruct{
			cond: "t1 AS t1 CROSS JOIN t2 AS t2 ON t1.id = t2.id",
			vals: nil,
		}},
	}

	ass := assert.New(t)
	for _, tc := range data {
		q, a := tc.in.Build()

		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func TestEmbed(t *testing.T) {
	type outStruct struct {
		cond string
		vals []interface{}
	}

	var data = []struct {
		in  Builder
		out outStruct
	}{
		{
			Embed("ifnull($,0) as amount", Raw("sum(case is_thaw when 0 then amount_thaw else 0)")),
			outStruct{
				cond: "ifnull(sum(case is_thaw when 0 then amount_thaw else 0),0) as amount",
				vals: nil,
			},
		},
		{
			Embed("max($,$,$,$)",
				Raw("?", 1),
				Raw("?", 2),
				Raw("?", 3),
				Raw("?", 4)),
			outStruct{
				cond: "max(?,?,?,?)",
				vals: []interface{}{1, 2, 3, 4},
			},
		},
	}

	ass := assert.New(t)
	for _, tc := range data {
		q, a := tc.in.Build()

		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func TestFunc(t *testing.T) {
	type outStruct struct {
		cond string
		vals []interface{}
	}

	var data = []struct {
		in  Builder
		out outStruct
	}{
		{
			Func("ifnull", Raw("col"), Raw("0")),
			outStruct{
				cond: "ifnull(col,0)",
				vals: nil,
			},
		},
		{
			Func("ifnull", Raw("?", 1), Raw("?", 2)),
			outStruct{
				cond: "ifnull(?,?)",
				vals: []interface{}{1, 2},
			},
		},
	}

	ass := assert.New(t)
	for _, tc := range data {
		q, a := tc.in.Build()

		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func TestSecComma_Build(t *testing.T) {
	type outStruct struct {
		cond string
		vals []interface{}
	}

	var data = []struct {
		in  Builder
		out outStruct
	}{
		{
			SecComma{Raw("a"), Raw("b"), Raw("c")},
			outStruct{
				cond: "a,b,c",
				vals: []interface{}{},
			},
		},
		{
			SecComma{Raw("?", 1), Raw("?", 2), Raw("?", 3)},
			outStruct{
				cond: "?,?,?",
				vals: []interface{}{1, 2, 3},
			},
		},
	}

	ass := assert.New(t)
	for _, tc := range data {
		q, a := tc.in.Build()

		ass.Equal(tc.out.cond, q)
		ass.Equal(tc.out.vals, a)
	}
}

func BenchmarkBuildSelect_Sequelization(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Select{
			Table:  Raw("tb"),
			Fields: []string{"a", "b", "c"},
			Where: SecAND{
				Raw("foo = ?", "bar"),
				Raw("qq = ?", "tt"),
				MakeIn("age", []interface{}{1, 3, 5, 7, 9}),
				Raw("faith <> ?", "Muslim"),
			},
			OrderBy: []string{"age desc"},
			GroupBy: []string{"department"},
			Limit:   []uint{0, 100},
		}.Build()
	}
}
