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

type Builder interface {
	Build() (string, []interface{})
}

type SecRaw struct {
	query string
	args  []interface{}
}

func (r SecRaw) Build() (string, []interface{}) {
	return string(r.query), r.args
}

type SecAND []Builder

func (a SecAND) Build() (string, []interface{}) {
	b := strings.Builder{}
	args := make([]interface{}, 0)
	b.WriteString("(")
	for i, v := range a {
		if i != 0 {
			b.WriteString(" AND ")
		}
		q, a := v.Build()
		b.WriteString(q)
		args = append(args, a...)
	}
	b.WriteString(")")
	return b.String(), args
}

type SecOR []Builder

func (o SecOR) Build() (string, []interface{}) {
	b := strings.Builder{}
	args := make([]interface{}, 0)
	b.WriteString("(")
	for i, v := range o {
		if i != 0 {
			b.WriteString(" OR ")
		}
		q, a := v.Build()
		b.WriteString(q)
		args = append(args, a...)
	}
	b.WriteString(")")
	return b.String(), args
}

func Raw(query string, args ...interface{}) Builder {
	return SecRaw{
		query: query,
		args:  args,
	}
}

func MakeAlias(b Builder, alias string) Builder {
	q, a := b.Build()
	if strings.ContainsRune(q, rune(' ')) {
		q = "(" + q + ")"
	}
	return SecRaw{
		query: q + " AS " + alias,
		args:  a,
	}
}

func MakeIn(col string, args []interface{}) Builder {
	return SecRaw{
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

	return SecRaw{
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

	return SecRaw{
		query: v + s + strings.Repeat(","+s, len(values)-1),
		args:  args,
	}, nil
}

func MakeSet(cols map[string]interface{}) Builder {
	set := SecRaw{}
	ss := make([]string, 0, len(cols))

	for k, v := range cols {
		ss = append(ss, k+"=?")
		set.args = append(set.args, v)
	}
	set.query = strings.Join(ss, ",")

	return set
}

func MakeSetSort(cols map[string]interface{}) Builder {
	set := SecRaw{}
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
	Fields  Builder
	Table   Builder
	Where   Builder
	GroupBy Builder
	Having  Builder
	OrderBy Builder
	Limit   Builder
}

func (s SelectRaw) Build() (string, []interface{}) {
	args := make([]interface{}, 0)

	fields, a := s.Fields.Build()
	args = append(args, a...)

	table, a := s.Table.Build()
	args = append(args, a...)

	where := ""
	if s.Where != nil {
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

	return "SELECT " + fields + " FROM " + table + where + groupBy + having + orderBy + limit, args
}

type Select struct {
	Fields  []string
	Table   Builder
	Where   Builder
	GroupBy string
	Having  Builder
	OrderBy []string
	Limit   []uint
}

func (s Select) Build() (string, []interface{}) {
	fields := Raw("*")
	if s.Fields != nil {
		fields = Raw(strings.Join(s.Fields, ","))
	}

	var groupBy, orderBy, limit Builder
	if s.GroupBy != "" {
		groupBy = Raw(s.GroupBy)
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
		Fields:  fields,
		Table:   s.Table,
		Where:   s.Where,
		GroupBy: groupBy,
		Having:  s.Having,
		OrderBy: orderBy,
		Limit:   limit,
	}.Build()
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
