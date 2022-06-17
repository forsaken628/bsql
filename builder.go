package bsql

import (
	"errors"
	"sort"
	"strings"
)

const (
	InnerJoin = iota
	LeftJoin
	RightJoin
	CrossJoin
)

type Nullable interface {
	Null() bool
}

func IsNull(b Builder) bool {
	if b == nil {
		return true
	}
	n, ok := b.(Nullable)
	if !ok {
		return false
	}
	return n.Null()
}

type Builder interface {
	Build() (string, []interface{})
}

type secRaw struct {
	query string
	args  []interface{}
}

func (r secRaw) Build() (string, []interface{}) {
	return r.query, r.args
}

type SecAND []Builder

func (a SecAND) Build() (string, []interface{}) {
	b := strings.Builder{}
	args := make([]interface{}, 0)
	b.WriteString("(")
	f := false
	for _, v := range a {
		if IsNull(v) {
			continue
		}
		if f {
			b.WriteString(" AND ")
		}
		f = true
		q, a := v.Build()
		b.WriteString(q)
		args = append(args, a...)
	}
	b.WriteString(")")
	return b.String(), args
}

func (a SecAND) Null() bool {
	if len(a) == 0 {
		return true
	}
	for _, b := range a {
		if !IsNull(b) {
			return false
		}
	}
	return true
}

type SecOR []Builder

func (o SecOR) Build() (string, []interface{}) {
	b := strings.Builder{}
	args := make([]interface{}, 0)
	b.WriteString("(")
	f := false
	for _, v := range o {
		if IsNull(v) {
			continue
		}
		if f {
			b.WriteString(" OR ")
		}
		f = true
		q, a := v.Build()
		b.WriteString(q)
		args = append(args, a...)
	}
	b.WriteString(")")
	return b.String(), args
}

func (o SecOR) Null() bool {
	if len(o) == 0 {
		return true
	}
	for _, b := range o {
		if !IsNull(b) {
			return false
		}
	}
	return true
}

func Raw(query string, args ...interface{}) Builder {
	return secRaw{
		query: query,
		args:  args,
	}
}

func EQ(col string, value interface{}) Builder {
	return secRaw{
		query: col + " = ?",
		args:  []interface{}{value},
	}
}

func NQ(col string, value interface{}) Builder {
	return secRaw{
		query: col + " != ?",
		args:  []interface{}{value},
	}
}

func GT(col string, value interface{}) Builder {
	return secRaw{
		query: col + " > ?",
		args:  []interface{}{value},
	}
}

func GTE(col string, value interface{}) Builder {
	return secRaw{
		query: col + " >= ?",
		args:  []interface{}{value},
	}
}

func LT(col string, value interface{}) Builder {
	return secRaw{
		query: col + " < ?",
		args:  []interface{}{value},
	}
}

func LTE(col string, value interface{}) Builder {
	return secRaw{
		query: col + " <= ?",
		args:  []interface{}{value},
	}
}

type SecCase struct {
	Case Builder
	When [][2]Builder
	Else Builder
}

func (c SecCase) Build() (string, []interface{}) {
	b := strings.Builder{}
	args := make([]interface{}, 0)
	b.WriteString("CASE")
	if c.Case != nil {
		q, a := c.Case.Build()
		b.WriteString(" " + q)
		args = append(args, a...)
	}
	for _, v := range c.When {
		b.WriteString(" WHEN ")
		q, a := v[0].Build()
		b.WriteString(q)
		args = append(args, a...)
		b.WriteString(" THEN ")
		q, a = v[1].Build()
		b.WriteString(q)
		args = append(args, a...)
	}
	if c.Else != nil {
		b.WriteString(" ELSE ")
		q, a := c.Else.Build()
		b.WriteString(q)
		args = append(args, a...)
	}
	b.WriteString(" END")
	return b.String(), args
}

func Func(fn string, builder ...Builder) Builder {
	raw := secRaw{}

	qs := make([]string, len(builder))
	for i, v := range builder {
		q, a := v.Build()
		qs[i] = q
		raw.args = append(raw.args, a...)
	}

	raw.query = fn + "(" + strings.Join(qs, ",") + ")"

	return raw
}

type SecComma []Builder

func (c SecComma) Build() (string, []interface{}) {
	b := strings.Builder{}
	args := make([]interface{}, 0)
	for i, v := range c {
		if i != 0 {
			b.WriteString(",")
		}
		q, a := v.Build()
		b.WriteString(q)
		args = append(args, a...)
	}
	return b.String(), args
}

func Embed(query string, builder ...Builder) Builder {
	raw := secRaw{}

	if strings.Count(query, "$") != len(builder) {
		panic("the number of places does not match")
	}

	end := len(query)
	argNum := 0
	for i := 0; i < end; i++ {
		lasti := i
		for i < end && query[i] != '$' {
			i++
		}
		if i > lasti {
			raw.query += query[lasti:i]
		}
		if i >= end {
			break
		}
		q, a := builder[argNum].Build()
		raw.query += q
		raw.args = append(raw.args, a...)
		argNum++
	}

	return raw
}

func MakeAlias(b Builder, alias string) Builder {
	q, a := b.Build()
	if strings.ContainsRune(q, ' ') {
		q = "(" + q + ")"
	}
	return secRaw{
		query: q + " AS " + alias,
		args:  a,
	}
}

func Bracket(b Builder) Builder {
	q, a := b.Build()
	return secRaw{
		query: "(" + q + ")",
		args:  a,
	}
}

func MakeIn(col string, args []interface{}) Builder {
	return secRaw{
		query: col + " IN (?" + strings.Repeat(",?", len(args)-1) + ")",
		args:  args,
	}
}

func MakeJoin(typ int8, t1, t2, on Builder) Builder {
	var args []interface{}

	qt1, a := t1.Build()
	args = append(args, a...)

	qt2, a := t2.Build()
	args = append(args, a...)

	qon := ""
	if on != nil {
		qon, a = on.Build()
		qon = " ON " + qon
		args = append(args, a...)
	}

	join := ""
	switch typ {
	case InnerJoin:
		join = " JOIN "
	case LeftJoin:
		join = " LEFT JOIN "
	case RightJoin:
		join = " RIGHT JOIN "
	case CrossJoin:
		join = " CROSS JOIN "
	default:
		panic("")
	}

	return secRaw{
		query: qt1 + join + qt2 + qon,
		args:  args,
	}
}

func MakeValues(cols []string, values [][]interface{}) (Builder, error) {
	if len(values) == 0 || len(values[0]) == 0 {
		return nil, errors.New("insert null values")
	}
	length := len(values[0])
	if cols != nil && len(cols) != length {
		return nil, errors.New("insert values not match")
	}

	args := make([]interface{}, 0, len(values)*len(values[0]))

	for _, v := range values {
		if len(v) != length {
			return nil, errors.New("insert values not match")
		}
		args = append(args, v...)
	}

	s := "(?" + strings.Repeat(",?", length-1) + ")"

	v := "VALUES "
	if len(cols) > 0 {
		v = "(" + strings.Join(cols, ",") + ") VALUES "
	}

	return secRaw{
		query: v + s + strings.Repeat(","+s, len(values)-1),
		args:  args,
	}, nil
}

func MakeSet(cols map[string]interface{}) Builder {
	set := secRaw{}
	ss := make([]string, 0, len(cols))

	for k, v := range cols {
		ss = append(ss, k+"=?")
		set.args = append(set.args, v)
	}
	set.query = strings.Join(ss, ",")

	return set
}

func MakeSetSort(cols map[string]interface{}) Builder {
	set := secRaw{}
	ss := make([]string, 0, len(cols))

	for k := range cols {
		ss = append(ss, k)
	}
	sort.Strings(ss)
	for k, v := range ss {
		ss[k] = v + "=?"
		set.args = append(set.args, cols[v])
	}
	set.query = strings.Join(ss, ",")

	return set
}

type SelectRaw struct {
	Distinct bool
	Fields   Builder
	Table    Builder
	Where    Builder
	GroupBy  Builder
	Having   Builder
	OrderBy  Builder
	Limit    Builder
}

func (s SelectRaw) Build() (string, []interface{}) {
	args := make([]interface{}, 0)

	fields, a := s.Fields.Build()
	args = append(args, a...)

	table, a := s.Table.Build()
	args = append(args, a...)

	where := ""
	if !IsNull(s.Where) {
		q, a := s.Where.Build()
		where = " WHERE " + q
		args = append(args, a...)
	}

	groupBy := ""
	if s.GroupBy != nil {
		q, a := s.GroupBy.Build()
		groupBy = " GROUP BY " + q
		args = append(args, a...)
	}

	having := ""
	if s.Having != nil {
		q, a := s.Having.Build()
		having = " HAVING " + q
		args = append(args, a...)
	}

	orderBy := ""
	if s.OrderBy != nil {
		q, a := s.OrderBy.Build()
		orderBy = " ORDER BY " + q
		args = append(args, a...)
	}

	limit := ""
	if s.Limit != nil {
		q, a := s.Limit.Build()
		limit = " LIMIT " + q
		args = append(args, a...)
	}

	sel := "SELECT "
	if s.Distinct {
		sel = "SELECT DISTINCT "
	}

	return sel + fields + " FROM " + table + where + groupBy + having + orderBy + limit, args
}

type Select struct {
	Distinct bool
	Fields   []string
	Table    Builder
	Where    Builder
	GroupBy  []string
	Having   Builder
	OrderBy  []string
	Limit    []uint
}

func (s Select) Build() (string, []interface{}) {
	fields := Raw("*")
	if s.Fields != nil {
		fields = Raw(strings.Join(s.Fields, ","))
	}

	var groupBy, orderBy, limit Builder
	if len(s.GroupBy) != 0 {
		groupBy = Raw(strings.Join(s.GroupBy, ","))
	}

	if s.OrderBy != nil {
		orderBy = Raw(strings.Join(s.OrderBy, ","))
	}

	if len(s.Limit) > 0 {
		if len(s.Limit) > 1 {
			limit = Raw("?,?", s.Limit[0], s.Limit[1])
		} else {
			limit = Raw("?", s.Limit[0])
		}
	}

	return SelectRaw{
		Distinct: s.Distinct,
		Fields:   fields,
		Table:    s.Table,
		Where:    s.Where,
		GroupBy:  groupBy,
		Having:   s.Having,
		OrderBy:  orderBy,
		Limit:    limit,
	}.Build()
}

type UnionAll []Select

func (ua UnionAll) Build() (string, []interface{}) {
	var (
		sqls    []string
		allArgs []interface{}
	)
	for _, s := range ua {
		sql, args := s.Build()
		sqls = append(sqls, sql)
		allArgs = append(allArgs, args...)
	}

	return strings.Join(sqls, " UNION ALL "), allArgs
}

type Update struct {
	Table Builder
	Set   Builder
	Where Builder
}

func (u Update) Build() (string, []interface{}) {
	args := make([]interface{}, 0)

	table, a := u.Table.Build()
	args = append(args, a...)

	set := ""
	if u.Set != nil {
		q, a := u.Set.Build()
		set = " SET " + q
		args = append(args, a...)
	}

	where := ""
	if u.Where != nil {
		q, a := u.Where.Build()
		where = " WHERE " + q
		args = append(args, a...)
	}

	return "UPDATE " + table + set + where, args
}

type Insert struct {
	Table Builder
	Value Builder
}

func (e Insert) Build() (string, []interface{}) {
	args := make([]interface{}, 0)

	table, a := e.Table.Build()
	args = append(args, a...)

	values := ""
	if e.Value != nil {
		values, a = e.Value.Build()
		args = append(args, a...)
	}

	return "INSERT INTO " + table + " " + values, args
}

type Delete struct {
	Table Builder
	Where Builder
}

func (d Delete) Build() (string, []interface{}) {
	args := make([]interface{}, 0)

	table, a := d.Table.Build()
	args = append(args, a...)

	where := ""
	if d.Where != nil {
		q, a := d.Where.Build()
		where = " WHERE " + q
		args = append(args, a...)
	}

	return "DELETE FROM " + table + where, args
}
