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
				GroupBy: "name",
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
				GroupBy: "name",
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
				GroupBy: "name",
				Having:  Raw("age > ?", 10),
				Limit:   []uint{0, 10},
			},
			out: outStruct{
				cond: "SELECT name,max(age) FROM tab WHERE (c6 != ?) GROUP BY name HAVING age > ? LIMIT ?,?",
				vals: []interface{}{"c6", 10, uint(0), uint(10)},
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
				Set: MakeSet(map[string]interface{}{
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

func TestMakeJoin(t *testing.T) {
	q, a := MakeJoin(InnerJoin,
		MakeAlias(Raw("t1"), "t1"),
		MakeAlias(Raw("t2"), "t2"),
		Raw("t1.id = t2.id"),
	).Build()

	ass := assert.New(t)
	ass.Equal("t1 AS t1 JOIN t2 AS t2 ON t1.id = t2.id", q)
	ass.Empty(a)
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
			GroupBy: "department",
			Limit:   []uint{0, 100},
		}.Build()
	}
}
